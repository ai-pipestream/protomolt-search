//! Document-keyed transpose of one field's postings ([`FieldTranspose`]).

use super::field_view::FieldView;
use crate::postings::{AnalyzedField, DocPositions, DocTerms};

/// One field of one sealed segment keyed by document instead of by
/// term: the postings
/// walked once in directory order and grouped by row, so a replay can
/// take a document's analyzed field from the store without the
/// analyzer. Flat arrays, one field of one segment at a time.
pub struct FieldTranspose {
    /// The directory's terms, in directory order.
    terms: Vec<String>,
    /// `(term index, tf, occurrence start, occurrence end, position start)`
    /// per posting, grouped by row through `row_starts`.
    entries: Vec<(u32, u32, u32, u32, u32)>,
    /// Prefix table: row `r`'s entries are `entries[row_starts[r]..row_starts[r + 1]]`.
    row_starts: Vec<u32>,
    /// Every occurrence span, indexed by an entry's occurrence range.
    occurrences: Vec<(u32, u32)>,
    /// Every ordinal, parallel to `occurrences`, when the field has positions.
    positions: Option<Vec<u32>>,
    pos_lengths: Option<Vec<u32>>,
    /// The field's doc-length table.
    lengths: Vec<u32>,
    /// Sentence tables per row, when the field has them.
    sentences: Option<Vec<Vec<(u32, u32)>>>,
    /// Quantized SPLADE impact scale (`f32::to_bits`), when the field carries SPLADE postings.
    splade_scales: Vec<u32>,
    /// Quantized SPLADE `(term, impact)` entries per row, when the field carries SPLADE postings.
    splade_by_row: Option<Vec<Vec<(String, u16)>>>,
}

impl FieldTranspose {
    /// Rows the transpose covers.
    pub fn rows(&self) -> usize {
        self.row_starts.len().saturating_sub(1)
    }

    /// Bytes the flat arrays take, the bound a replay reports.
    pub fn bytes(&self) -> usize {
        self.splade_scales.len() * 4
            + self.entries.len() * 20
            + self.occurrences.len() * 8
            + self.positions.as_ref().map_or(0, |p| p.len() * 4)
            + self.pos_lengths.as_ref().map_or(0, |p| p.len() * 4)
            + self.row_starts.len() * 4
            + self.lengths.len() * 4
            + self.terms.iter().map(|t| t.len() + 24).sum::<usize>()
            + self
                .sentences
                .as_ref()
                .map_or(0, |s| s.iter().map(|t| t.len() * 8 + 24).sum())
            + self
                .splade_by_row
                .as_ref()
                .map_or(0, |rows| rows.iter().map(|v| v.len() * 32 + 24).sum())
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
        let mut pos_lengths: Option<DocPositions> = self
            .pos_lengths
            .as_ref()
            .map(|_| Vec::with_capacity(to - from));
        for &(term, tf, occ_start, occ_end, pos_start) in &self.entries[from..to] {
            let spans = self.occurrences[occ_start as usize..occ_end as usize].to_vec();
            let n = (occ_end - occ_start) as usize;
            if let (Some(all), Some(out)) = (&self.positions, positions.as_mut()) {
                out.push(all[pos_start as usize..pos_start as usize + n].to_vec());
            }
            if let (Some(all), Some(out)) = (&self.pos_lengths, pos_lengths.as_mut()) {
                out.push(all[pos_start as usize..pos_start as usize + n].to_vec());
            }
            terms.push((self.terms[term as usize].clone(), tf, spans));
        }
        let splade = self
            .splade_by_row
            .as_ref()
            .and_then(|rows| rows.get(r))
            .cloned()
            .unwrap_or_default();
        let splade_scale_bits = if splade.is_empty() {
            0
        } else {
            self.splade_scales[r]
        };
        Some(AnalyzedField {
            terms,
            length: self.lengths.get(r).copied().unwrap_or(0),
            positions,
            pos_lengths,
            sentences: self.sentences.as_ref().map(|s| s[r].clone()),
            splade,
            splade_scale_bits,
        })
    }
}

impl FieldView<'_> {
    /// The field keyed by physical slot: one
    /// walk of the directory in term order, then a counting sort of postings
    /// by slot across `0..doc_lengths.len()`.
    pub fn transpose(&self) -> Result<FieldTranspose, String> {
        let r = self.reader;
        let rows = self.field.doc_lengths.len();
        let (terms, row_starts, total_postings, total_occurrences) =
            self.count_transpose_rows(rows)?;
        let mut cursors = row_starts.clone();
        let posted = self.fill_transpose_postings(&mut cursors, total_postings, total_occurrences);
        let fi = r
            .fields
            .iter()
            .position(|f| std::ptr::eq(f, self.field))
            .expect("a field view borrows one of its reader's fields");
        let sentences = self.field.sentences_off.map(|_| {
            (0..rows as u32)
                .map(|row| r.field_doc_sentences(fi, row).unwrap_or_default())
                .collect()
        });
        let splade = self.transpose_splade_rows(rows)?;
        Ok(FieldTranspose {
            terms,
            entries: posted.entries,
            row_starts,
            occurrences: posted.occurrences,
            positions: posted.positions,
            pos_lengths: posted.pos_lengths,
            lengths: self.field.doc_lengths.clone(),
            sentences,
            splade_scales: splade.scales,
            splade_by_row: splade.by_row,
        })
    }

    fn count_transpose_rows(
        &self,
        rows: usize,
    ) -> Result<(Vec<String>, Vec<u32>, usize, usize), String> {
        let r = self.reader;
        let n_terms = self.field.n_terms;
        let mut terms = Vec::with_capacity(n_terms as usize);
        let mut counts = vec![0u32; rows + 1];
        let mut total_postings = 0usize;
        let mut total_occurrences = 0usize;
        for i in 0..n_terms {
            let (bytes, doc_run_off, _, _, df) = r.directory_entry(self.field, i);
            terms.push(String::from_utf8_lossy(bytes).into_owned());
            let doc_run_off = doc_run_off as usize;
            for j in 0..df as usize {
                let (doc, _, _) = r.doc_entry(doc_run_off, j);
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
            let occ_end = r.occ_start(doc_run_off, df as usize, df as usize) as usize;
            let occ_first = if df == 0 {
                occ_end
            } else {
                r.occ_start(doc_run_off, df as usize, 0) as usize
            };
            total_occurrences += occ_end - occ_first;
        }
        for slot in 1..counts.len() {
            counts[slot] += counts[slot - 1];
        }
        Ok((terms, counts, total_postings, total_occurrences))
    }

    fn fill_transpose_postings(
        &self,
        cursors: &mut [u32],
        total_postings: usize,
        total_occurrences: usize,
    ) -> TransposedPostings {
        let r = self.reader;
        let n_terms = self.field.n_terms;
        let positions_section = self.field.positions_off.map(|off| off as usize);
        let pos_lengths_section = self.field.pos_lengths_off.map(|off| off as usize);
        let mut entries = vec![(0u32, 0u32, 0u32, 0u32, 0u32); total_postings];
        let mut occurrences: Vec<(u32, u32)> = Vec::with_capacity(total_occurrences);
        let mut positions = positions_section.map(|_| Vec::with_capacity(total_occurrences));
        let mut pos_lengths = pos_lengths_section.map(|_| Vec::with_capacity(total_occurrences));
        let data_off = |sec: usize| sec + 4 + 4 * (n_terms as usize + 1);
        for i in 0..n_terms {
            let (_, doc_run_off, _, occ_run_off, df) = r.directory_entry(self.field, i);
            let (doc_run_off, occ_run_off) = (doc_run_off as usize, occ_run_off as usize);
            let pos_sec =
                positions_section.map(|s| (r.u32_at(s + 4 + 4 * i as usize), data_off(s)));
            let len_sec =
                pos_lengths_section.map(|s| (r.u32_at(s + 4 + 4 * i as usize), data_off(s)));
            for j in 0..df as usize {
                let (doc, tf, occ_start) = r.doc_entry(doc_run_off, j);
                let occ_end = r.occ_start(doc_run_off, df as usize, j + 1);
                let flat_start = occurrences.len() as u32;
                for o in occ_start..occ_end {
                    let e = occ_run_off + 8 * o as usize;
                    occurrences.push((r.u32_at(e), r.u32_at(e + 4)));
                }
                let pos_start = positions.as_ref().map_or(0, |p| p.len() as u32);
                if let (Some(out), Some((base, data))) = (positions.as_mut(), pos_sec) {
                    for o in occ_start..occ_end {
                        out.push(r.u32_at(data + 4 * (base as usize + o as usize)));
                    }
                }
                if let (Some(out), Some((base, data))) = (pos_lengths.as_mut(), len_sec) {
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
        TransposedPostings {
            entries,
            occurrences,
            positions,
            pos_lengths,
        }
    }

    fn transpose_splade_rows(&self, rows: usize) -> Result<TransposedSplade, String> {
        let Some((off, len)) = self.field.splade_range else {
            return Ok(TransposedSplade {
                scales: Vec::new(),
                by_row: None,
            });
        };
        let bytes = &self.reader.map[off as usize..(off + len) as usize];
        let decoded = crate::turbo_bridge::splade::SpladeFieldIndex::decode(bytes)
            .map_err(|e| format!("field {:?}: invalid SPLADE section: {e}", self.field.name))?;
        let scales = (0..rows)
            .map(|row| decoded.doc_scale_bits(row as u32))
            .collect();
        let mut by_row: Vec<Vec<(String, u16)>> = vec![Vec::new(); rows];
        for (term, plist) in decoded.postings {
            for (doc, q) in plist {
                let slot = doc as usize;
                if slot >= rows {
                    return Err(format!(
                        "field {:?}: SPLADE term {:?} names row {doc} of a {rows}-row store",
                        self.field.name, term
                    ));
                }
                by_row[slot].push((term.clone(), q));
            }
        }
        Ok(TransposedSplade {
            scales,
            by_row: Some(by_row),
        })
    }
}

struct TransposedPostings {
    entries: Vec<(u32, u32, u32, u32, u32)>,
    occurrences: Vec<(u32, u32)>,
    positions: Option<Vec<u32>>,
    pos_lengths: Option<Vec<u32>>,
}

struct TransposedSplade {
    scales: Vec<u32>,
    by_row: Option<Vec<Vec<(String, u16)>>>,
}
