//! Field-table width/name agreement, positional storage checks, and analyzer fingerprints.

use crate::node::*;
use crate::postings::{AnalyzedDoc, StoredBinding};

impl NodeServiceImpl {
    /// Validate active field-table compatibility and record per-column analyzer
    /// fingerprints before any document row enters the store.
    pub(super) fn validate_and_record_analysis(
        &self,
        guard: &mut ShardState,
        doc: &AddDocumentsRequest,
        analyzed: &AnalyzedDoc,
    ) -> Result<(), Status> {
        {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            validate_field_table(&self.config, shard, analyzed)?;
            validate_positional_storage(&self.config, shard, doc, analyzed)?;
        }
        let shard = guard.bm25.as_mut().expect("builder just ensured");
        if let Some(binding) = &guard.mapped_binding {
            apply_mapped_binding_fingerprints(shard, binding, doc)?;
        }
        record_document_fingerprints(&self.config, self.phrase_index.as_deref(), shard, doc)
    }
}

fn validate_field_table(
    config: &NodeConfig,
    shard: &Bm25Shard,
    analyzed: &AnalyzedDoc,
) -> Result<(), Status> {
    if analyzed.fields.len() <= 1 {
        return Ok(());
    }
    if analyzed.fields.len() > shard.field_count() {
        return Err(Status::failed_precondition(format!(
            "document carries {} fields but the shard's table has {}; the shard \
             predates the configured field table — rebuild or reshard it",
            analyzed.fields.len(),
            shard.field_count()
        )));
    }
    for fi in 1..analyzed.fields.len() {
        let want = config.bm25_fields.get(fi).map(String::as_str);
        if want != Some(shard.field_name(fi)) {
            return Err(Status::failed_precondition(format!(
                "shard field {fi} is {:?} but the configured table names {:?}; \
                 field tables must agree",
                shard.field_name(fi),
                want.unwrap_or("<missing>")
            )));
        }
    }
    Ok(())
}

fn validate_positional_storage(
    config: &NodeConfig,
    shard: &Bm25Shard,
    doc: &AddDocumentsRequest,
    analyzed: &AnalyzedDoc,
) -> Result<(), Status> {
    for name in &doc.position_fields {
        let Some(fi) = config.bm25_fields.iter().position(|n| n == name) else {
            continue;
        };
        if fi < shard.field_count() && !shard.field_has_positions(fi) {
            return Err(Status::failed_precondition(format!(
                "shard field {name:?} predates token positions (its storage declares \
                 none); rebuild or reshard the generation before positional ingest"
            )));
        }
    }
    for name in &doc.sentence_fields {
        let Some(fi) = config.bm25_fields.iter().position(|n| n == name) else {
            continue;
        };
        if fi < shard.field_count() && !shard.field_has_sentences(fi) {
            return Err(Status::failed_precondition(format!(
                "shard field {name:?} predates sentence spans (its storage declares \
                 none); rebuild or reshard the generation before sentence ingest"
            )));
        }
    }
    for (fi, field) in analyzed.fields.iter().enumerate() {
        if fi < shard.field_count()
            && shard.field_has_positions(fi)
            && !field.terms.is_empty()
            && field.positions.is_none()
        {
            return Err(Status::failed_precondition(format!(
                "shard field {:?} keeps token positions but this document's analysis \
                 carried none; positions are never guessed from spans",
                shard.field_name(fi)
            )));
        }
    }
    Ok(())
}

fn apply_mapped_binding_fingerprints(
    shard: &mut Bm25Shard,
    binding: &StoredBinding,
    doc: &AddDocumentsRequest,
) -> Result<(), Status> {
    let contract = crate::mapped_analysis::decode_contract(
        &binding.analysis_sha,
        &binding.analysis_contract,
        &binding.body_path,
    )
    .map_err(Status::failed_precondition)?;
    for field in &contract.fields {
        let offered = if field.name == "body" {
            Some(doc.analysis.as_ref())
        } else {
            doc.fields
                .iter()
                .find(|value| value.field == field.name)
                .map(|value| value.analysis.as_ref())
        };
        if offered.is_some_and(|spec| spec != field.analysis.as_ref()) {
            return Err(Status::failed_precondition(format!(
                "field {:?} analysis does not match the explicit mapped binding",
                field.name
            )));
        }
    }
    for field in contract.fields {
        let fi = (0..shard.field_count())
            .find(|&fi| shard.field_name(fi) == field.name)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "bound analysis field {:?} is absent from the shard",
                    field.name
                ))
            })?;
        shard
            .set_analysis_fingerprint(
                fi,
                crate::analyzer::analysis_fingerprint(field.analysis.as_ref()),
            )
            .map_err(Status::failed_precondition)?;
    }
    Ok(())
}

fn record_document_fingerprints(
    config: &NodeConfig,
    phrase_index: Option<&crate::phrases::PhraseIndex>,
    shard: &mut Bm25Shard,
    doc: &AddDocumentsRequest,
) -> Result<(), Status> {
    let body = crate::analyzer::analysis_fingerprint(doc.analysis.as_ref());
    shard
        .set_analysis_fingerprint(0, body)
        .map_err(Status::failed_precondition)?;
    if let Some(ci) = cased_field_index(config, phrase_index, doc)? {
        let spec = doc
            .analysis
            .as_ref()
            .expect("cased_field_index requires an explicit body spec");
        let twin = crate::analyzer::cased_twin_spec(spec);
        shard
            .set_analysis_fingerprint(ci, crate::analyzer::analysis_fingerprint(Some(&twin)))
            .map_err(Status::failed_precondition)?;
    }
    for field in &doc.fields {
        let Some(fi) = config.bm25_fields.iter().position(|n| *n == field.field) else {
            continue;
        };
        let fingerprint = crate::analyzer::analysis_fingerprint(field.analysis.as_ref());
        shard
            .set_analysis_fingerprint(fi, fingerprint)
            .map_err(Status::failed_precondition)?;
    }
    if let Some(phrases) = phrase_index {
        let fi = config
            .bm25_fields
            .iter()
            .position(|field| field == phrases.phrase_field())
            .expect("phrase field validated at configuration");
        shard
            .set_analysis_fingerprint(fi, phrases.fingerprint())
            .map_err(Status::failed_precondition)?;
    }
    for bigram in &doc.bigram_fields {
        let (Some(source), Some(derived)) = (
            config.bm25_fields.iter().position(|n| *n == bigram.source),
            config.bm25_fields.iter().position(|n| *n == bigram.field),
        ) else {
            continue;
        };
        let source_fingerprint = shard.analysis_fingerprint(source);
        if source_fingerprint != 0 {
            shard
                .set_analysis_fingerprint(
                    derived,
                    crate::proximity::bigram_fingerprint(source_fingerprint),
                )
                .map_err(Status::failed_precondition)?;
        }
    }
    Ok(())
}
