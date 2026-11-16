//! Streaming field-analysis coordination and mapped/plain document intake.

use crate::node::*;

mod field_streams;
mod source;
mod streamed;

pub(in crate::node) use self::field_streams::{FieldEvent, FieldStreams};
pub(in crate::node) use self::source::{IngestDoc, IngestSource, MappedSource, PendingDoc};

/// One document whose analysis has landed, ready to apply.
pub(in crate::node) struct AnalyzedRow {
    pub(in crate::node) doc: AddDocumentsRequest,
    pub(in crate::node) analyzed: crate::postings::AnalyzedDoc,
    pub(in crate::node) vector: Option<Vec<f32>>,
    pub(in crate::node) stable_routing_key: Option<Vec<u8>>,
    /// The document came from a WAL replay stream.
    pub(in crate::node) logged: bool,
}

impl NodeServiceImpl {
    pub(in crate::node) fn doc_session_layers(
        &self,
        doc: &AddDocumentsRequest,
    ) -> crate::analyzer::SessionLayers {
        let mut layers = session_layers(
            doc,
            self.phrase_index.as_deref(),
            &self.config.sentence_fields,
        );
        layers.nlp_columns = doc.nlp_materialization.is_empty()
            && self
                .nlp_columns
                .as_deref()
                .is_some_and(crate::turbo_bridge::NlpColumnSpec::is_enabled);
        layers
    }
}

/// The optional sidecar layers a document's specs ask its analysis
/// session for — the session-identity companion to the reopen
/// condition (a change to either spec reopens the session).
pub(crate) fn session_layers(
    doc: &AddDocumentsRequest,
    phrase_index: Option<&crate::phrases::PhraseIndex>,
    sentence_fields: &[String],
) -> crate::analyzer::SessionLayers {
    crate::analyzer::SessionLayers {
        quality: quality_wanted(doc.quality.as_ref()),
        geography: geography_wanted(doc.geography.as_ref()),
        entities: phrase_index.is_some_and(crate::phrases::PhraseIndex::include_ner),
        // Request sentence spans whenever the node is configured to store them.
        sentences: !sentence_fields.is_empty(),
        dual_cased: !doc.cased_field.is_empty(),
        nlp_columns: false,
    }
}

impl NodeServiceImpl {
    /// Hold every extra field of `doc` to the shard's field table, returning
    /// each field's table index in document order. The whole document is
    /// validated before anything is submitted: a partially submitted document
    /// would leave orphan sequences in flight that nothing ever routes.
    fn validate_extra_fields(&self, doc: &AddDocumentsRequest) -> Result<Vec<usize>, Status> {
        let mut seen: Vec<&str> = Vec::new();
        let mut accepted: Vec<usize> = Vec::with_capacity(doc.fields.len());
        for field in &doc.fields {
            if field.field == "body" {
                return Err(Status::invalid_argument(
                    "\"body\" is the top-level text; DocumentField names extra fields only",
                ));
            }
            if self
                .phrase_index
                .as_ref()
                .is_some_and(|phrases| field.field == phrases.phrase_field())
            {
                return Err(Status::invalid_argument(format!(
                    "field {:?} is derived from the configured phrase glossary; clients must not supply it",
                    field.field
                )));
            }
            if let Some(source) = self
                .config
                .bigram_fields
                .iter()
                .find(|source| crate::proximity::bigram_field_name(source) == field.field)
            {
                return Err(Status::invalid_argument(format!(
                    "field {:?} is the bigram column derived from {source:?}; clients must not supply it",
                    field.field
                )));
            }
            if seen.contains(&field.field.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "field {:?} repeats in one document",
                    field.field
                )));
            }
            if !doc.cased_field.is_empty() && field.field == doc.cased_field {
                return Err(Status::invalid_argument(format!(
                    "field {:?} is the body's cased identity (cased_field), produced from the \
                     body's analysis; do not supply it as a DocumentField",
                    field.field
                )));
            }
            seen.push(&field.field);
            let Some(fi) = self
                .config
                .bm25_fields
                .iter()
                .position(|n| *n == field.field)
            else {
                return Err(Status::invalid_argument(format!(
                    "unknown field {:?}; this shard indexes {:?}",
                    field.field, self.config.bm25_fields
                )));
            };
            if field.text.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "field {:?} has empty text; omit absent fields instead",
                    field.field
                )));
            }
            accepted.push(fi);
        }
        Ok(accepted)
    }

    /// Validate a document's extra fields against the shard's configured
    /// field table and queue their analyses on the per-spec field
    /// streams. Validation failures surface BEFORE any store or WAL
    /// effect, and before anything is submitted.
    ///
    /// Every field's sequence is recorded in `route` so its result can be
    /// steered back to `(document sequence, slot in that document's
    /// extras)`; the sequence is the only key the wire carries.
    pub(in crate::node) async fn submit_field_analyses(
        &self,
        doc: &AddDocumentsRequest,
        sequence: u64,
        streams: &mut FieldStreams,
        route: &mut std::collections::HashMap<u64, (u64, usize)>,
    ) -> Result<Vec<(usize, Option<crate::postings::AnalyzedField>)>, Status> {
        // A cased field is validated here, before the body is submitted,
        // so a misnamed one is refused without analyzing anything.
        cased_field_index(&self.config, self.phrase_index.as_deref(), doc)?;
        if doc.fields.is_empty() {
            return Ok(Vec::new());
        }
        let accepted = self.validate_extra_fields(doc)?;
        let mut extras = Vec::with_capacity(accepted.len());
        for (slot, (field, fi)) in doc.fields.iter().zip(accepted).enumerate() {
            let tag = streams.submit(field.analysis.as_ref(), &field.text).await?;
            route.insert(tag, (sequence, slot));
            extras.push((fi, None));
        }
        Ok(extras)
    }

    /// Apply one analyzed document: id assignment, store insert, WAL
    /// append. Must be called in arrival order — both transports
    /// guarantee it.
    pub(in crate::node) async fn apply_analyzed_document(
        &self,
        row: AnalyzedRow,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        let AnalyzedRow {
            doc,
            analyzed,
            vector,
            stable_routing_key,
            logged,
        } = row;
        // Refuse live requests carrying forged derived column values or
        // fingerprints before placement checks and column materialization
        // run. A logged row keeps its values; materialization holds its
        // fingerprint to this index's declaration.
        if let Some(declaration) = self.config.derived.as_deref().filter(|_| !logged) {
            declaration.refuse_carried(&doc)?;
        }
        self.check_placement(&doc, stable_routing_key.as_deref())?;
        let (doc, analyzed) =
            self.materialize_document(doc, analyzed, stable_routing_key.as_deref())?;
        let _mutation = self.mutation_gate.read().await;
        let mut guard = write_shard(&self.state);
        self.apply_document_locked(
            &mut guard,
            doc,
            analyzed,
            vector,
            stable_routing_key,
            added,
            first_id,
        )
    }

    /// Advance the apply wavefront over every consecutive sequence whose
    /// body AND every extra field have landed, keeping application in
    /// arrival order.
    pub(in crate::node) async fn advance_apply(
        &self,
        pending: &mut std::collections::BTreeMap<u64, PendingDoc>,
        results: &mut std::collections::HashMap<u64, crate::postings::AnalyzedDoc>,
        next_apply: &mut u64,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        loop {
            let ready = results.contains_key(next_apply)
                && pending.get(next_apply).is_some_and(|p| p.outstanding == 0);
            if !ready || self.tail_full() {
                return Ok(());
            }
            let analyzed = results.remove(next_apply).expect("readiness just checked");
            let held = pending.remove(next_apply).expect("readiness just checked");
            let cased = cased_field_index(&self.config, self.phrase_index.as_deref(), &held.doc)?;
            let analyzed = join_fields(analyzed, held.extras, cased)?;
            let row = AnalyzedRow {
                doc: held.doc,
                analyzed,
                vector: held.vector,
                stable_routing_key: held.stable_routing_key,
                logged: held.logged,
            };
            self.apply_analyzed_document(row, added, first_id).await?;
            *next_apply += 1;
        }
    }
}
