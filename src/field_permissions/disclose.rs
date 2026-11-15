//! The disclosure pass over a lexical response.
use super::FieldScope;
use crate::pb::*;
use tonic::Status;

impl FieldScope {
    /// Explicit detail requests require permission; automatic details may be
    /// omitted with a visible redaction flag while preserving the ranking.
    pub(crate) fn disclose(&self, response: &mut Bm25SearchResponse) -> Result<(), Status> {
        let mut redacted = false;
        for hit in &mut response.hits {
            let Bm25Hit {
                doc_id: _,
                score: _,
                terms,
                projected: _,
                snippets,
                explain,
                identity,
            } = hit;
            terms.retain(|term| {
                let field = if term.field.is_empty() {
                    "body"
                } else {
                    &term.field
                };
                let keep = self.can_disclose(field);
                redacted |= !keep;
                keep
            });
            for snippet in snippets {
                self.require_disclose(&snippet.field)?;
            }
            if let Some(explain) = explain {
                for term in &explain.terms {
                    self.require_disclose(&term.field)?;
                }
                for stage in &explain.stages {
                    self.require_disclose(&stage.column)?;
                }
            }
            if !self.disclose_identity && identity.take().is_some() {
                redacted = true;
            }
        }
        response.synonym_expansions.retain(|expansion| {
            let keep = self.can_disclose(&expansion.field);
            redacted |= !keep;
            keep
        });
        response.phrase_routing.retain(|route| {
            let keep = self.can_disclose(&route.field) && self.can_disclose(&route.served_field);
            redacted |= !keep;
            keep
        });
        response.field_details_redacted = redacted;
        Ok(())
    }
}
