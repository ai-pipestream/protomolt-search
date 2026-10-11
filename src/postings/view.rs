//! Per-field read surfaces over an open file: `FieldView` presents one field as
//! a `Bm25Index`, and `FieldTranspose` iterates a field's postings column-wise
//! for facet and aggregation passes.

use super::*;

/// One field of an open `.bm25` file as its own [`Bm25Index`]
/// (`docs/multi-field.md`): directory lookups, postings walks, and
/// impact cursors run against the field's sections; texts, lineages,
/// and the document count are the file's shared ones.
#[derive(Clone, Copy)]
pub struct FieldView<'a> {
    pub(crate) reader: &'a Bm25Reader,
    pub(crate) field: &'a FieldSlice,
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
    pub(crate) fn impacts_inner(self, term: &str) -> Option<ImpactCursor<'a>> {
        self.file_impacts(term).map(ImpactCursor::single)
    }

    /// This file's own cursor for `term`, the part a segmented shard
    /// chains (`src/segmented.rs`); `None` without v5 skip runs or
    /// without the term.
    pub(crate) fn file_impacts(self, term: &str) -> Option<FileImpacts<'a>> {
        if !self.reader.v5_runs {
            return None;
        }
        let (doc_run_off, skip_run_off, occ_run_off, df) =
            self.reader.directory_lookup_v5(self.field, term)?;
        Some(FileImpacts::new(
            &self.reader.map,
            doc_run_off as usize,
            occ_run_off as usize,
            skip_run_off as usize,
            df,
        ))
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
        if self.reader.v5_runs {
            self.reader
                .directory_lookup_v5(self.field, term)
                .map_or(0, |(_, _, _, df)| df)
        } else {
            self.reader
                .directory_lookup(self.field, term)
                .map_or(0, |(_, df)| df)
        }
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        let r = self.reader;
        if r.v5_runs {
            let Some((doc_run_off, _, occ_run_off, df)) = r.directory_lookup_v5(self.field, term)
            else {
                return;
            };
            let (doc_run_off, occ_run_off) = (doc_run_off as usize, occ_run_off as usize);
            for i in 0..df as usize {
                let (doc_id, tf, occ_start) = r.v5_doc_entry(doc_run_off, i);
                let occ_end = r.v5_occ_start(doc_run_off, df as usize, i + 1);
                let offsets = r.v5_occ_slice(occ_run_off, occ_start, occ_end);
                f(doc_id, tf, &offsets);
            }
        } else {
            let Some((off, df)) = r.directory_lookup(self.field, term) else {
                return;
            };
            r.v3_for_each_posting(off, df, f);
        }
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        let r = self.reader;
        if r.v5_runs {
            let Some((doc_run_off, _, _, df)) = r.directory_lookup_v5(self.field, term) else {
                return;
            };
            r.v5_for_each_doc_tf(doc_run_off as usize, df, f);
        } else {
            self.for_each_posting(term, &mut |doc_id, tf, _offsets| {
                f(doc_id, tf);
            });
        }
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let r = self.reader;
        if r.v5_runs {
            let Some((doc_run_off, _, occ_run_off, df)) = r.directory_lookup_v5(self.field, term)
            else {
                return Vec::new();
            };
            // Binary search the fixed-stride doc run (doc ids ascending).
            let doc_run_off = doc_run_off as usize;
            let (mut lo, mut hi) = (0usize, df as usize);
            while lo < hi {
                let mid = (lo + hi) / 2;
                if r.u32_at(doc_run_off + 12 * mid) < doc_id {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            let i = lo;
            if i >= df as usize || r.u32_at(doc_run_off + 12 * i) != doc_id {
                return Vec::new();
            }
            let occ_start = r.u32_at(doc_run_off + 12 * i + 8);
            let occ_end = r.v5_occ_start(doc_run_off, df as usize, i + 1);
            r.v5_occ_slice(occ_run_off as usize, occ_start, occ_end)
        } else {
            // v3/v4: sequential walk with early exit — postings are
            // doc-id-ordered by construction, so stop at the first doc
            // past the target; offset bytes before it are stepped over
            // undecoded. (The trait default walks the whole list, which
            // costs O(df) per survivor at k=1000.)
            let Some((off, df)) = r.directory_lookup(self.field, term) else {
                return Vec::new();
            };
            let mut cur = off as usize;
            let term_len = u32::from_le_bytes(r.map[cur..cur + 4].try_into().unwrap()) as usize;
            cur += 4 + term_len + 4; // term header + posting count
            for _ in 0..df {
                let doc = r.u32_at(cur);
                let n_offsets = r.u32_at(cur + 8) as usize;
                if doc == doc_id {
                    let mut offsets = Vec::with_capacity(n_offsets);
                    let mut o = cur + 12;
                    for _ in 0..n_offsets {
                        offsets.push((r.u32_at(o), r.u32_at(o + 4)));
                        o += 8;
                    }
                    return offsets;
                }
                if doc > doc_id {
                    return Vec::new();
                }
                cur += 12 + 8 * n_offsets;
            }
            Vec::new()
        }
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        (*self).impacts_inner(term)
    }
    fn has_impacts(&self, term: &str) -> bool {
        self.reader.v5_runs && self.reader.directory_lookup_v5(self.field, term).is_some()
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
        if !r.v5_runs {
            return None;
        }
        let i = r.directory_index_v5(self.field, term)?;
        let (_, doc_run_off, _, _, df) = r.directory_entry_v5(self.field, i);
        let doc_run_off = doc_run_off as usize;
        let j = r.v5_posting_index(doc_run_off, df, doc_id)?;
        let Some(section) = self.field.positions_off else {
            return Some(Vec::new());
        };
        // base[i] + occ_start .. base[i] + occ_end, past the term count
        // and the base table (see COLUMN_KIND_POSITIONS).
        let section = section as usize;
        let base = r.u32_at(section + 4 + 4 * i as usize) as usize;
        let data = section + 4 + 4 * (self.field.n_terms as usize + 1);
        let occ_start = r.u32_at(doc_run_off + 12 * j + 8) as usize;
        let occ_end = r.v5_occ_start(doc_run_off, df as usize, j + 1) as usize;
        Some(
            (occ_start..occ_end)
                .map(|o| r.u32_at(data + 4 * (base + o)))
                .collect(),
        )
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

/// One field of one sealed segment keyed by document instead of by
/// term (`docs/replay-from-segments.md`, "The transpose"): the postings
/// walked once in directory order and grouped by row, so a replay can
/// take a document's analyzed field from the store without the
/// analyzer. Flat arrays, one field of one segment at a time.
pub struct FieldTranspose {
    /// The directory's terms, in directory order.
    pub(crate) terms: Vec<String>,
    /// `(term index, tf, occurrence start, occurrence end, position start)`
    /// per posting, grouped by row through `row_starts`.
    pub(crate) entries: Vec<(u32, u32, u32, u32, u32)>,
    /// Prefix table: row `r`'s entries are `entries[row_starts[r]..row_starts[r + 1]]`.
    pub(crate) row_starts: Vec<u32>,
    /// Every occurrence span, indexed by an entry's occurrence range.
    pub(crate) occurrences: Vec<(u32, u32)>,
    /// Every ordinal, parallel to `occurrences`, when the field has positions.
    pub(crate) positions: Option<Vec<u32>>,
    /// The field's doc-length table.
    pub(crate) lengths: Vec<u32>,
    /// Sentence tables per row, when the field has them.
    pub(crate) sentences: Option<Vec<Vec<(u32, u32)>>>,
}

impl FieldTranspose {
    /// Rows the transpose covers.
    pub fn rows(&self) -> usize {
        self.row_starts.len().saturating_sub(1)
    }

    /// Bytes the flat arrays take, the bound a replay reports.
    pub fn bytes(&self) -> usize {
        self.entries.len() * 20
            + self.occurrences.len() * 8
            + self.positions.as_ref().map_or(0, |p| p.len() * 4)
            + self.row_starts.len() * 4
            + self.lengths.len() * 4
            + self.terms.iter().map(|t| t.len() + 24).sum::<usize>()
            + self
                .sentences
                .as_ref()
                .map_or(0, |s| s.iter().map(|t| t.len() * 8 + 24).sum())
    }

    /// Whether the field carries positions.
    pub fn has_positions(&self) -> bool {
        self.positions.is_some()
    }

    /// Whether the field carries sentence tables.
    pub fn has_sentences(&self) -> bool {
        self.sentences.is_some()
    }

    /// Row `row`'s analyzed field: its terms in directory order with
    /// their tf and spans, the ordinals when the field has positions,
    /// the sentence table when it has one, and the stored length. A row
    /// with no term (an empty field) is a default field with its length.
    pub fn field(&self, row: u32) -> Option<AnalyzedField> {
        let r = row as usize;
        if r + 1 >= self.row_starts.len() {
            return None;
        }
        let (from, to) = (self.row_starts[r] as usize, self.row_starts[r + 1] as usize);
        let mut terms: DocTerms = Vec::with_capacity(to - from);
        let mut positions: Option<DocPositions> = self
            .positions
            .as_ref()
            .map(|_| Vec::with_capacity(to - from));
        for &(term, tf, occ_start, occ_end, pos_start) in &self.entries[from..to] {
            let spans = self.occurrences[occ_start as usize..occ_end as usize].to_vec();
            if let (Some(all), Some(out)) = (&self.positions, positions.as_mut()) {
                let n = (occ_end - occ_start) as usize;
                out.push(all[pos_start as usize..pos_start as usize + n].to_vec());
            }
            terms.push((self.terms[term as usize].clone(), tf, spans));
        }
        Some(AnalyzedField {
            terms,
            length: self.lengths.get(r).copied().unwrap_or(0),
            positions,
            sentences: self.sentences.as_ref().map(|s| s[r].clone()),
        })
    }
}

impl FieldView<'_> {
    /// The field keyed by row (`docs/replay-from-segments.md`): one walk
    /// of the directory in term order, then a counting sort of the
    /// postings by row. Refused by name on a file without skip runs,
    /// whose postings would have to be re-parsed per term.
    pub fn transpose(&self) -> Result<FieldTranspose, String> {
        let r = self.reader;
        if !r.v5_runs {
            return Err(format!(
                "field {:?}: the store has no skip runs (a v3/v4 file); a transpose needs the \
                 v5 directory, rebuild the source first",
                self.field.name
            ));
        }
        let rows = r.doc_count as usize;
        let n_terms = self.field.n_terms;
        let positions_section = self.field.positions_off.map(|off| off as usize);
        let mut terms = Vec::with_capacity(n_terms as usize);
        // First walk: count postings per row, and total occurrences.
        let mut counts = vec![0u32; rows + 1];
        let mut total_postings = 0usize;
        let mut total_occurrences = 0usize;
        for i in 0..n_terms {
            let (bytes, doc_run_off, _, _, df) = r.directory_entry_v5(self.field, i);
            terms.push(String::from_utf8_lossy(bytes).into_owned());
            let doc_run_off = doc_run_off as usize;
            for j in 0..df as usize {
                let (doc, _, _) = r.v5_doc_entry(doc_run_off, j);
                let slot = doc as usize;
                if slot >= rows {
                    return Err(format!(
                        "field {:?}: term {:?} names row {doc} of a {rows}-row store",
                        self.field.name, terms[i as usize]
                    ));
                }
                counts[slot + 1] += 1;
                total_postings += 1;
            }
            let occ_end = r.v5_occ_start(doc_run_off, df as usize, df as usize) as usize;
            let occ_first = if df == 0 {
                occ_end
            } else {
                r.v5_occ_start(doc_run_off, df as usize, 0) as usize
            };
            total_occurrences += occ_end - occ_first;
        }
        let mut row_starts = counts;
        for slot in 1..row_starts.len() {
            row_starts[slot] += row_starts[slot - 1];
        }
        // Second walk: place each posting at its row's cursor, copying
        // its spans (and ordinals) into the flat arrays.
        let mut cursors = row_starts.clone();
        let mut entries = vec![(0u32, 0u32, 0u32, 0u32, 0u32); total_postings];
        let mut occurrences: Vec<(u32, u32)> = Vec::with_capacity(total_occurrences);
        let mut positions: Option<Vec<u32>> =
            positions_section.map(|_| Vec::with_capacity(total_occurrences));
        let positions_data =
            positions_section.map(|section| section + 4 + 4 * (n_terms as usize + 1));
        for i in 0..n_terms {
            let (_, doc_run_off, _, occ_run_off, df) = r.directory_entry_v5(self.field, i);
            let (doc_run_off, occ_run_off) = (doc_run_off as usize, occ_run_off as usize);
            let base = positions_section.map(|section| r.u32_at(section + 4 + 4 * i as usize));
            for j in 0..df as usize {
                let (doc, tf, occ_start) = r.v5_doc_entry(doc_run_off, j);
                let occ_end = r.v5_occ_start(doc_run_off, df as usize, j + 1);
                let flat_start = occurrences.len() as u32;
                for o in occ_start..occ_end {
                    let e = occ_run_off + 8 * o as usize;
                    occurrences.push((r.u32_at(e), r.u32_at(e + 4)));
                }
                let pos_start = positions.as_ref().map_or(0, |p| p.len() as u32);
                if let (Some(out), Some(data), Some(base)) =
                    (positions.as_mut(), positions_data, base)
                {
                    for o in occ_start..occ_end {
                        out.push(r.u32_at(data + 4 * (base as usize + o as usize)));
                    }
                }
                let slot = doc as usize;
                let at = cursors[slot] as usize;
                cursors[slot] += 1;
                entries[at] = (i, tf, flat_start, occurrences.len() as u32, pos_start);
            }
        }
        let fi = r
            .fields
            .iter()
            .position(|f| std::ptr::eq(f, self.field))
            .expect("a field view borrows one of its reader's fields");
        let sentences = if self.field.sentences_off.is_some() {
            let mut tables = Vec::with_capacity(rows);
            for row in 0..rows as u32 {
                tables.push(r.field_doc_sentences(fi, row).unwrap_or_default());
            }
            Some(tables)
        } else {
            None
        };
        Ok(FieldTranspose {
            terms,
            entries,
            row_starts,
            occurrences,
            positions,
            lengths: self.field.doc_lengths.clone(),
            sentences,
        })
    }

    /// The directory entry's term bytes, on either directory shape.
    pub(crate) fn directory_term(&self, i: u32) -> &[u8] {
        let r = self.reader;
        if r.v5_runs {
            r.directory_entry_v5(self.field, i).0
        } else {
            r.directory_entry(self.field, i).0
        }
    }

    /// The directory entry's df, on either directory shape: the posting
    /// list's length as written, tombstones included.
    pub(crate) fn directory_df(&self, i: u32) -> u32 {
        let r = self.reader;
        if r.v5_runs {
            r.directory_entry_v5(self.field, i).4
        } else {
            r.directory_entry(self.field, i).2
        }
    }

    /// Lower bound of `prefix` in the byte-sorted directory: the index
    /// of the first term at or above it. O(log n) directory reads.
    pub(crate) fn prefix_lower_bound(&self, prefix: &str) -> u32 {
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
    /// O(log n) plus O(matches), never the whole dictionary
    /// (`docs/prefix-terms.md`).
    pub(crate) fn scan_prefix(
        &self,
        prefix: &str,
        mut visit: impl FnMut(usize, u32, &[u8]),
    ) -> usize {
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
