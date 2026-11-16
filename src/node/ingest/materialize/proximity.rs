//! Token-position, sentence-span, and bigram proximity derivation.

use crate::node::*;
use crate::proximity::{bigram_field_name, derive_bigrams};

/// Whether BM25 field `fi` was analyzed without occurrences: the body's spec,
/// or the extra field's own. Positions need FULL vectors.
fn scoring_only(doc: &AddDocumentsRequest, table: &[String], fi: usize) -> bool {
    let spec = if fi == 0 {
        doc.analysis.as_ref()
    } else {
        doc.fields
            .iter()
            .find(|f| f.field == table[fi])
            .and_then(|f| f.analysis.as_ref())
    };
    spec.is_some_and(|s| s.term_vector_mode == crate::analyzer::TERM_VECTOR_MODE_SCORING_ONLY)
}

impl NodeServiceImpl {
    fn configured_bigrams(&self) -> Vec<crate::pb::BigramField> {
        self.config
            .bigram_fields
            .iter()
            .map(|source| crate::pb::BigramField {
                source: source.clone(),
                field: bigram_field_name(source),
            })
            .collect()
    }

    /// Hold a pre-populated proximity record to this node's configured
    /// sentence, position, and bigram fields.
    fn check_recorded_proximity(
        &self,
        doc: &AddDocumentsRequest,
        configured_bigrams: &[crate::pb::BigramField],
    ) -> Result<(), Status> {
        let mut held: Vec<&str> = doc.sentence_fields.iter().map(String::as_str).collect();
        let mut want: Vec<&str> = self
            .config
            .sentence_fields
            .iter()
            .map(String::as_str)
            .collect();
        held.sort_unstable();
        want.sort_unstable();
        if held != want {
            return Err(Status::failed_precondition(format!(
                "document records sentence spans on {held:?} but this node keeps them on \
                 {want:?}; rebuild or replay with the matching --sentence-fields"
            )));
        }
        let mut held: Vec<&str> = doc.position_fields.iter().map(String::as_str).collect();
        let mut want: Vec<&str> = self
            .config
            .position_fields
            .iter()
            .map(String::as_str)
            .collect();
        held.sort_unstable();
        want.sort_unstable();
        if held != want {
            return Err(Status::failed_precondition(format!(
                "document records token positions on {held:?} but this node keeps them on \
                 {want:?}; rebuild or replay with the matching --position-fields"
            )));
        }
        let mut held: Vec<(&str, &str)> = doc
            .bigram_fields
            .iter()
            .map(|b| (b.source.as_str(), b.field.as_str()))
            .collect();
        let mut want: Vec<(&str, &str)> = configured_bigrams
            .iter()
            .map(|b| (b.source.as_str(), b.field.as_str()))
            .collect();
        held.sort_unstable();
        want.sort_unstable();
        if held != want {
            return Err(Status::failed_precondition(format!(
                "document records bigram columns {held:?} but this node derives {want:?}; \
                 rebuild or replay with the matching --bigram-fields"
            )));
        }
        Ok(())
    }

    fn proximity_field_index(&self, name: &str) -> Result<usize, Status> {
        let table = &self.config.bm25_fields;
        table.iter().position(|n| n == name).ok_or_else(|| {
            Status::failed_precondition(format!(
                "proximity field {name:?} is absent from this node's BM25 field table {table:?}"
            ))
        })
    }

    fn require_positions(
        &self,
        doc: &AddDocumentsRequest,
        analyzed: &crate::postings::AnalyzedDoc,
        fi: usize,
        role: &str,
    ) -> Result<(), Status> {
        let table = &self.config.bm25_fields;
        let Some(field) = analyzed.fields.get(fi) else {
            // The document does not carry this field at all: there
            // is nothing to position and nothing to derive.
            return Ok(());
        };
        if field.terms.is_empty() {
            return Ok(());
        }
        if scoring_only(doc, table, fi) {
            return Err(Status::invalid_argument(format!(
                "{role} {:?} requires FULL term vectors: a SCORING_ONLY analysis has no \
                 occurrences to place, so no phrase could ever match it",
                table[fi]
            )));
        }
        if field.positions.is_none() {
            return Err(Status::failed_precondition(format!(
                "{role} {:?} needs token positions, but the analysis of this document carried \
                 no token layer to derive them from; positions are never guessed from spans",
                table[fi]
            )));
        }
        field.check_positions().map_err(|error| {
            Status::invalid_argument(format!(
                "{role} {:?}: malformed token positions: {error}",
                table[fi]
            ))
        })
    }

    /// Sentence fields require sorted, non-overlapping sentence spans covering
    /// every indexed occurrence.
    fn check_sentence_fields(
        &self,
        doc: &AddDocumentsRequest,
        analyzed: &crate::postings::AnalyzedDoc,
    ) -> Result<(), Status> {
        for name in &doc.sentence_fields {
            let fi = self.proximity_field_index(name)?;
            let Some(field) = analyzed.fields.get(fi) else {
                continue;
            };
            if field.sentences.is_none() {
                return Err(Status::failed_precondition(format!(
                    "sentence field {name:?}: the analysis carried no sentence layer, so the \
                     document cannot be indexed with sentence spans; the analysis backend must \
                     return its sentence layer (the sidecar's sentence_detection)"
                )));
            }
            field.check_sentences().map_err(|error| {
                Status::failed_precondition(format!(
                    "sentence field {name:?}: malformed sentence spans: {error}"
                ))
            })?;
        }
        Ok(())
    }

    fn derive_bigram_columns(
        &self,
        doc: &AddDocumentsRequest,
        analyzed: &mut crate::postings::AnalyzedDoc,
    ) -> Result<(), Status> {
        for bigram in &doc.bigram_fields {
            let source = self.proximity_field_index(&bigram.source)?;
            let derived = self.proximity_field_index(&bigram.field)?;
            if bigram.field != bigram_field_name(&bigram.source) {
                return Err(Status::invalid_argument(format!(
                    "bigram column {:?} must be named {:?}",
                    bigram.field,
                    bigram_field_name(&bigram.source)
                )));
            }
            self.require_positions(doc, analyzed, source, "bigram source field")?;
            let Some(source_field) = analyzed.fields.get(source) else {
                continue;
            };
            if source_field.terms.is_empty() {
                continue;
            }
            let column = derive_bigrams(source_field).map_err(|error| {
                Status::invalid_argument(format!(
                    "bigram column {:?} from {:?}: {error}",
                    bigram.field, bigram.source
                ))
            })?;
            if analyzed.fields.len() <= derived {
                analyzed.fields.resize_with(derived + 1, Default::default);
            }
            if analyzed.fields[derived] != crate::postings::AnalyzedField::default() {
                return Err(Status::invalid_argument(format!(
                    "bigram column {:?} collides with supplied analyzed data",
                    bigram.field
                )));
            }
            analyzed.fields[derived] = column;
        }
        Ok(())
    }

    /// Derive token-position and bigram proximity payloads and pin the
    /// document's durable proximity record. Fresh records fill from node
    /// configuration; pre-populated records must match configuration and carry
    /// full positional term vectors.
    pub(in crate::node) fn materialize_proximity(
        &self,
        mut doc: AddDocumentsRequest,
        mut analyzed: crate::postings::AnalyzedDoc,
    ) -> Result<(AddDocumentsRequest, crate::postings::AnalyzedDoc), Status> {
        // Enforce single-collection membership and stamp unnamed documents before logging.
        self.admit_collection(&doc.collection)?;
        doc.collection = self.config.collection.clone();
        let configured_bigrams = self.configured_bigrams();
        let fresh = doc.position_fields.is_empty()
            && doc.bigram_fields.is_empty()
            && doc.sentence_fields.is_empty();
        if fresh {
            doc.position_fields = self.config.position_fields.clone();
            doc.bigram_fields = configured_bigrams;
            doc.sentence_fields = self.config.sentence_fields.clone();
        } else {
            self.check_recorded_proximity(&doc, &configured_bigrams)?;
        }
        if doc.position_fields.is_empty()
            && doc.bigram_fields.is_empty()
            && doc.sentence_fields.is_empty()
        {
            return Ok((doc, analyzed));
        }
        for name in &doc.position_fields {
            let fi = self.proximity_field_index(name)?;
            self.require_positions(&doc, &analyzed, fi, "positional field")?;
        }
        self.check_sentence_fields(&doc, &analyzed)?;
        self.derive_bigram_columns(&doc, &mut analyzed)?;
        Ok((doc, analyzed))
    }
}
