//! Analyzed documents as ingest hands them to the postings builders.

/// Term data for one document, as produced from the sidecar's term
/// vectors: `(term, tf, original-text offsets)`.
pub type DocTerms = Vec<(String, u32, Vec<(u32, u32)>)>;

/// Token ordinals per occurrence, parallel to a [`DocTerms`] table:
/// `positions[i][j]` is the ordinal of `terms[i].2[j]`. An ordinal counts
/// every token the tokenizer produced, emitted or not, so ordinal
/// adjacency is token adjacency — the fact a phrase query needs and a
/// pair of character spans cannot state.
pub type DocPositions = Vec<Vec<u32>>;

/// One analyzed field of one document: per-term data plus the field's
/// length in terms (sum of frequencies, the BM25 length-normalization
/// input).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnalyzedField {
    /// Per-term data; see [`DocTerms`].
    pub terms: DocTerms,
    /// Field length in terms.
    pub length: u32,
    /// Token ordinals parallel to `terms` (see [`DocPositions`]), or
    /// `None` when the analysis carried no token layer to derive them
    /// from. A positional field refuses a `None` document by name rather
    /// than indexing it without positions, because half a column with
    /// positions is a phrase index that lies about the other half.
    pub positions: Option<DocPositions>,
    /// Sentence spans of the field's text in UTF-8 bytes, sorted and
    /// non-overlapping, or `None` when the analysis carried no sentence
    /// layer. A sentence field refuses a `None` document by name;
    /// every occurrence must lie inside one
    /// sentence, which [`Self::check_sentences`] enforces at ingest so
    /// the query path can trust the table.
    pub sentences: Option<Vec<(u32, u32)>>,
    /// Optional token-lattice arc lengths (`pos_len >= 1`) parallel to
    /// `positions`. `None` when every token has unit length
    /// and distinct ordinals; `Some` activates lattice traversal and kind-18
    /// `.bm25` persistence.
    pub pos_lengths: Option<DocPositions>,
    /// Optional calibrated quantized SPLADE `(term, impact)` postings for this
    /// field.
    pub splade: Vec<(String, u16)>,
    /// IEEE-754 `f32::to_bits(scale)` for `splade` (`0` when empty).
    pub splade_scale_bits: u32,
}

impl AnalyzedField {
    /// Check that `sentences`, when present, is a sorted, non-overlapping
    /// table of non-empty spans covering every occurrence of every term.
    pub fn check_sentences(&self) -> Result<(), String> {
        let Some(sentences) = &self.sentences else {
            return Ok(());
        };
        crate::highlight::check_sentence_table(sentences)?;
        let occurrences = self
            .terms
            .iter()
            .flat_map(|(_, _, offsets)| offsets.iter().copied());
        crate::highlight::check_coverage(sentences, occurrences).map_err(|(start, end)| {
            format!("occurrence [{start}, {end}) lies outside every sentence span")
        })
    }

    /// Check that `positions` (and optional `pos_lengths`), when present, is
    /// shaped exactly like the term table: one list per term, one ordinal per
    /// occurrence, each list strictly ascending.
    pub fn check_positions(&self) -> Result<(), String> {
        let Some(positions) = &self.positions else {
            if self.pos_lengths.is_some() {
                return Err("pos_lengths present without positions".into());
            }
            return Ok(());
        };
        if positions.len() != self.terms.len() {
            return Err(format!(
                "positions table has {} entries for {} terms",
                positions.len(),
                self.terms.len()
            ));
        }
        for ((term, _, offsets), ordinals) in self.terms.iter().zip(positions) {
            if ordinals.len() != offsets.len() {
                return Err(format!(
                    "term {term:?} has {} occurrences but {} positions",
                    offsets.len(),
                    ordinals.len()
                ));
            }
            if ordinals.windows(2).any(|pair| pair[1] <= pair[0]) {
                return Err(format!(
                    "term {term:?} positions are not strictly ascending"
                ));
            }
        }
        if let Some(pos_lengths) = &self.pos_lengths {
            if pos_lengths.len() != self.terms.len() {
                return Err(format!(
                    "pos_lengths table has {} entries for {} terms",
                    pos_lengths.len(),
                    self.terms.len()
                ));
            }
            for ((term, _, offsets), lengths) in self.terms.iter().zip(pos_lengths) {
                if lengths.len() != offsets.len() {
                    return Err(format!(
                        "term {term:?} has {} occurrences but {} pos_lengths",
                        offsets.len(),
                        lengths.len()
                    ));
                }
                if lengths.contains(&0) {
                    return Err(format!("term {term:?} has zero pos_len"));
                }
            }
        }
        Ok(())
    }
}

/// One analyzed document ready to be indexed: one entry per field,
/// positionally matching the store's field table. Field 0 is the body
/// (the stored text). A document may carry fewer entries than the
/// store has fields; missing trailing fields index as empty.
#[derive(Debug, Clone, Default)]
pub struct AnalyzedDoc {
    /// Per-field analyzed data, indexed by field id.
    pub fields: Vec<AnalyzedField>,
    /// The body's cased term identity from the SAME analysis pass,
    /// when the ingest asked for one: the same
    /// tokens under the same chain minus case folding, spans and
    /// ordinals coinciding with the body's. The node places it at the
    /// field the request named; it is never a second analysis.
    pub cased: Option<AnalyzedField>,
    /// Per-document quality scalars from the sidecar's noise and
    /// artifact layers, when the ingest asked for them.
    /// `None` means they were not
    /// requested, which is deliberately distinct from a clean document
    /// (all zeros): the ingest writes a column only when it has a real
    /// measurement for it.
    pub quality: Option<DocQuality>,
    /// Per-document geography reduction from the sidecar's geocoding
    /// layer, when the ingest asked for it.
    /// `None` means not requested;
    /// `Some` with empty fields means measured-and-found-nothing,
    /// which materializes as column ABSENCE (there is no neutral
    /// coordinate to write).
    pub geography: Option<DocGeography>,
    /// Named-entity mentions retained from the sidecar until ingest
    /// materializes them into an ordinary map column. They are never mixed
    /// into body postings.
    pub entities: Vec<crate::phrases::DocEntity>,
    /// Optional one-pass TurboEmbed NLP projection for
    /// pre-WAL materialization of typed columns.
    pub nlp: Option<Box<turbo_client::DocumentNlpProjection>>,
}

impl PartialEq for AnalyzedDoc {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
            && self.cased == other.cased
            && self.quality == other.quality
            && self.geography == other.geography
            && self.entities == other.entities
            && self.nlp.is_some() == other.nlp.is_some()
    }
}

/// One document's geography reduction, derived at INGEST and stored
/// as ordinary typed columns. Every
/// field is optional on purpose: a document that mentions no
/// resolvable place has nothing honest to store.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DocGeography {
    /// Best resolved location `(lat, lon)`: the highest-confidence
    /// finding, ties broken by text order.
    pub point: Option<(f64, f64)>,
    /// The chosen location's resolution confidence in [0, 1].
    /// Meaningless without `point`; read it only when `point` is set.
    pub confidence: f64,
    /// Top region vote's ISO country code, empty when the document's
    /// location evidence voted for nothing.
    pub country: String,
}

/// One document's quality measurements, all derived at INGEST and
/// stored as ordinary typed columns. The
/// query path never recomputes them and never calls a model.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct DocQuality {
    /// Worst noise finding's score, in [0, 1]; exactly 0 with no
    /// findings.
    pub noise: f64,
    /// Characters covered by the UNION of the noise findings' spans.
    pub noise_chars: i64,
    /// Number of flagged text artifacts.
    pub artifacts: i64,
}

impl AnalyzedDoc {
    /// A document with only the body field — every single-field ingest
    /// path.
    pub fn body(terms: DocTerms, length: u32) -> Self {
        Self {
            fields: vec![AnalyzedField {
                terms,
                length,
                positions: None,
                sentences: None,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// [`Self::body`] with the token positions the analysis produced.
    pub fn body_positioned(terms: DocTerms, positions: DocPositions, length: u32) -> Self {
        Self {
            fields: vec![AnalyzedField {
                terms,
                length,
                positions: Some(positions),
                sentences: None,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// The body field's analyzed data, consuming the document (empty
    /// when the document carried no fields).
    pub fn into_body(self) -> AnalyzedField {
        self.fields.into_iter().next().unwrap_or_default()
    }
}

pub(super) fn clone_parallel_ordinals(
    table: &Option<Vec<Vec<u32>>>,
    term_idx: usize,
    expected_len: usize,
    label: &str,
) -> Vec<u32> {
    match table {
        Some(rows) => {
            assert_eq!(
                rows[term_idx].len(),
                expected_len,
                "{label} must be parallel to offsets"
            );
            rows[term_idx].clone()
        }
        None => Vec::new(),
    }
}
