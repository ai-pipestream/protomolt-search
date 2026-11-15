//! Folding a token lattice into one field's per-term postings.

use std::collections::{HashMap, HashSet};

use crate::postings::{AnalyzedField, DocPositions, DocTerms};

/// Accumulates lattice arcs into a field's terms, positions, and position
/// lengths, in first-occurrence term order.
pub(super) struct LatticeFold {
    scoring_only: bool,
    term_indices: HashMap<String, usize>,
    terms: DocTerms,
    positions: DocPositions,
    pos_lengths: DocPositions,
    seen_ordinals: HashSet<u32>,
    has_lattice: bool,
    primary_len: u32,
    max_tf: u32,
}

impl LatticeFold {
    pub(super) fn new(scoring_only: bool) -> Self {
        Self {
            scoring_only,
            term_indices: HashMap::new(),
            terms: Vec::new(),
            positions: Vec::new(),
            pos_lengths: Vec::new(),
            seen_ordinals: HashSet::new(),
            has_lattice: false,
            primary_len: 0,
            max_tf: 0,
        }
    }

    /// Adds one arc with a non-empty term at position `pos`, covering
    /// `pos_len` positions and the source bytes `span`.
    pub(super) fn push<T>(&mut self, term: T, pos: u32, pos_len: u32, span: (u32, u32))
    where
        T: AsRef<str> + Into<String>,
    {
        let arc_len = pos_len.max(1);
        if arc_len == 1 {
            self.primary_len = self.primary_len.saturating_add(1);
        } else {
            self.has_lattice = true;
        }
        match self.term_indices.get(term.as_ref()).copied() {
            Some(idx) => self.push_repeat(idx, pos, arc_len, span),
            None => {
                if !self.seen_ordinals.insert(pos) {
                    self.has_lattice = true;
                }
                let idx = self.terms.len();
                let term: String = term.into();
                self.term_indices.insert(term.clone(), idx);
                self.max_tf = self.max_tf.max(1);
                let occs = if self.scoring_only {
                    Vec::new()
                } else {
                    vec![span]
                };
                self.terms.push((term, 1, occs));
                self.positions.push(vec![pos]);
                self.pos_lengths.push(vec![arc_len]);
            }
        }
    }

    fn push_repeat(&mut self, idx: usize, pos: u32, arc_len: u32, span: (u32, u32)) {
        // Per-term positions stay strictly ascending: an arc that lands on
        // the position already recorded last for this term widens that
        // occurrence's source span and arc length instead of repeating the
        // ordinal.
        if let Some(last_pos) = self.positions[idx].last().copied() {
            if pos <= last_pos {
                if pos == last_pos && !self.scoring_only {
                    if let Some(last_occ) = self.terms[idx].2.last_mut() {
                        last_occ.0 = last_occ.0.min(span.0);
                        last_occ.1 = last_occ.1.max(span.1);
                    }
                    if let Some(last_len) = self.pos_lengths[idx].last_mut() {
                        *last_len = (*last_len).max(arc_len);
                    }
                }
                return;
            }
        }
        if !self.seen_ordinals.insert(pos) {
            self.has_lattice = true;
        }
        self.terms[idx].1 += 1;
        self.max_tf = self.max_tf.max(self.terms[idx].1);
        if !self.scoring_only {
            self.terms[idx].2.push(span);
        }
        self.positions[idx].push(pos);
        self.pos_lengths[idx].push(arc_len);
    }

    /// The finished field over `sentences`, non-empty spans in ascending
    /// order.
    pub(super) fn finish(self, mut sentences: Vec<(u32, u32)>) -> AnalyzedField {
        // An occurrence can bridge two adjacent sentences (a word rejoined
        // across a line-break hyphen); those sentences merge so every
        // occurrence lies inside one sentence span.
        for (_, _, occs) in &self.terms {
            for &(os, oe) in occs {
                cover_occurrence(&mut sentences, os, oe);
            }
        }
        AnalyzedField {
            terms: self.terms,
            length: self.primary_len.max(self.max_tf),
            positions: if self.scoring_only {
                None
            } else {
                Some(self.positions)
            },
            pos_lengths: if !self.scoring_only && self.has_lattice {
                Some(self.pos_lengths)
            } else {
                None
            },
            sentences: Some(sentences),
            ..Default::default()
        }
    }
}

/// Widens, merges, or appends sentence spans until one of them encloses the
/// occurrence `[os, oe)`.
fn cover_occurrence(sentences: &mut Vec<(u32, u32)>, os: u32, oe: u32) {
    if os >= oe {
        return;
    }
    let first = sentences.partition_point(|&(_, se)| se <= os);
    let last_excl = sentences.partition_point(|&(ss, _)| ss < oe);
    if first < sentences.len() && last_excl > first + 1 {
        let merged_start = sentences[first].0.min(os);
        let merged_end = sentences[last_excl - 1].1.max(oe);
        sentences.splice(first..last_excl, [(merged_start, merged_end)]);
    } else if first < sentences.len() {
        sentences[first].0 = sentences[first].0.min(os);
        sentences[first].1 = sentences[first].1.max(oe);
    } else {
        sentences.push((os, oe));
    }
}
