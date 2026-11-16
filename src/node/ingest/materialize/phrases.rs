//! Phrase-glossary posting derivation, validation, and collection admission.

use crate::node::*;

/// Stamp a fresh document with the glossary's postings and identity, or hold
/// a replay-ready document to the configured vocabulary. Returns whether the
/// document was fresh.
fn stamp_or_check_phrase_identity(
    index: &crate::phrases::PhraseIndex,
    doc: &mut AddDocumentsRequest,
) -> Result<bool, Status> {
    let expected = index.fingerprint();
    let fresh = doc.phrase_fingerprint == 0;
    if fresh {
        if !doc.phrases.is_empty() {
            return Err(Status::invalid_argument(
                "client-supplied phrase postings require their non-zero vocabulary fingerprint",
            ));
        }
        doc.phrases = index.postings(&doc.text);
        doc.phrase_fingerprint = expected;
        doc.phrase_field = index.phrase_field().to_string();
    } else if doc.phrase_fingerprint != expected {
        return Err(Status::failed_precondition(format!(
            "document phrase fingerprint {:016x} differs from configured vocabulary {:016x}; rebuild or replay with the matching glossary",
            doc.phrase_fingerprint, expected
        )));
    } else if doc.phrase_field != index.phrase_field() {
        return Err(Status::failed_precondition(format!(
            "document phrase field {:?} differs from configured field {:?}",
            doc.phrase_field,
            index.phrase_field()
        )));
    }
    Ok(fresh)
}

fn check_phrase_postings(
    index: &crate::phrases::PhraseIndex,
    doc: &AddDocumentsRequest,
) -> Result<(), Status> {
    for posting in &doc.phrases {
        if posting.term != protomolt_analyzer::phrase_posting_term(&posting.concept_id)
            || posting.field != index.phrase_field()
            || posting.concept_id.is_empty()
            || posting.token_count == 0
            || posting.offsets.is_empty()
            || posting.offsets.iter().any(|span| span.start >= span.end)
        {
            return Err(Status::invalid_argument(format!(
                "invalid durable phrase posting for concept {:?}",
                posting.concept_id
            )));
        }
    }
    Ok(())
}

/// Fold glossary and NER entity entries into the document's map facets,
/// refusing a derived key that conflicts with a client value.
fn merge_derived_entities(
    index: &crate::phrases::PhraseIndex,
    doc: &mut AddDocumentsRequest,
    analyzed: &crate::postings::AnalyzedDoc,
) -> Result<(), Status> {
    let mut derived = index.glossary_entities(&doc.phrases);
    if let Some(field) = index.entity_map_field() {
        derived.extend(crate::phrases::ner_entities(field, &analyzed.entities));
    }
    for entry in derived {
        match doc
            .map_facets
            .iter()
            .find(|held| held.field == entry.field && held.key == entry.key)
        {
            Some(held) if held.value == entry.value => {}
            Some(_) => {
                return Err(Status::invalid_argument(format!(
                    "derived entity map key {:?} conflicts with a client value",
                    entry.key
                )))
            }
            None => doc.map_facets.push(entry),
        }
    }
    Ok(())
}

impl NodeServiceImpl {
    /// Derive or validate phrase postings, materialize entity map entries on
    /// the first pass, and install the dedicated analyzed field. The returned
    /// request is the durable WAL form.
    pub(in crate::node) fn materialize_phrases(
        &self,
        mut doc: AddDocumentsRequest,
        mut analyzed: crate::postings::AnalyzedDoc,
    ) -> Result<(AddDocumentsRequest, crate::postings::AnalyzedDoc), Status> {
        let Some(index) = &self.phrase_index else {
            if !doc.phrases.is_empty()
                || doc.phrase_fingerprint != 0
                || !doc.phrase_field.is_empty()
            {
                return Err(Status::failed_precondition(
                    "document carries derived phrase data but this node has no phrase glossary configured",
                ));
            }
            return Ok((doc, analyzed));
        };
        let fresh = stamp_or_check_phrase_identity(index, &mut doc)?;
        check_phrase_postings(index, &doc)?;
        // A non-zero fingerprint marks a replay-ready document. Its entity
        // entries were already derived before the WAL append, so never run a
        // possibly changed NER model over it again.
        if fresh {
            merge_derived_entities(index, &mut doc, &analyzed)?;
        }
        let field_index = self
            .config
            .bm25_fields
            .iter()
            .position(|field| field == index.phrase_field())
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "configured phrase field {:?} is absent from this node's BM25 field table",
                    index.phrase_field()
                ))
            })?;
        if analyzed.fields.len() <= field_index {
            analyzed
                .fields
                .resize_with(field_index + 1, Default::default);
        }
        if analyzed.fields[field_index] != crate::postings::AnalyzedField::default() {
            return Err(Status::invalid_argument(format!(
                "derived phrase field {:?} collides with supplied analyzed data",
                index.phrase_field()
            )));
        }
        analyzed.fields[field_index] = crate::phrases::analyzed_field(&doc.phrases);
        Ok((doc, analyzed))
    }

    /// Admit a write only for this node's collection: an empty name means
    /// this node's collection, and any other name refuses.
    pub(in crate::node) fn admit_collection(&self, requested: &str) -> Result<(), Status> {
        if requested.is_empty() || requested == self.config.collection {
            return Ok(());
        }
        Err(if self.config.collection.is_empty() {
            Status::invalid_argument(format!(
                "collection {requested:?} named, but this node serves no named collection"
            ))
        } else {
            Status::invalid_argument(format!(
                "this node serves collection {:?}, not {requested:?}",
                self.config.collection
            ))
        })
    }
}
