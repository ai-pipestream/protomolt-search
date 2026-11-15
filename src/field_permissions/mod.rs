//! Field-use and disclosure checks for authority-issued search decisions.
use crate::pb::*;
use std::collections::BTreeMap;
use tonic::Status;

mod disclose;
mod query;
mod routes;

#[derive(Clone, Debug)]
pub(crate) struct FieldScope {
    grants: BTreeMap<String, u8>,
    disclose_identity: bool,
    /// The index's derived columns: a column declared `inputs` is usable
    /// or disclosable only when the same action is granted on every
    /// column it reads, and on the document identity when it reads the
    /// stable key. Hashing a field does not make it public.
    derived: Option<std::sync::Arc<crate::derived::Declaration>>,
}
impl FieldScope {
    /// Attach the declaration the grants are judged under.
    pub(crate) fn with_derived(
        mut self,
        derived: Option<std::sync::Arc<crate::derived::Declaration>>,
    ) -> Self {
        self.derived = derived;
        self
    }
    /// The inputs a derived column's grant depends on: `None` for a
    /// source column or a column declared `own`; otherwise its input
    /// columns and whether it reads the stable key.
    fn derived_inputs(&self, field: &str) -> Option<(&[String], bool)> {
        let column = self.derived.as_ref()?.column(field)?;
        match column.disclosure {
            crate::pb::DerivedDisclosure::Inputs => Some((&column.inputs, column.reads_stable_key)),
            _ => None,
        }
    }
    pub(crate) fn new(input: &FieldPermissions) -> Result<Self, String> {
        let mut grants = BTreeMap::new();
        for grant in &input.grants {
            if grant.field.is_empty() || grant.actions.is_empty() {
                return Err("field grants require a name and at least one action".into());
            }
            let mut bits = 0;
            for action in &grant.actions {
                let bit = match FieldAction::try_from(*action) {
                    Ok(FieldAction::Use) => 1,
                    Ok(FieldAction::Disclose) => 2,
                    _ => return Err("unknown field action".into()),
                };
                if bits & bit != 0 {
                    return Err("field grant repeats an action".into());
                }
                bits |= bit;
            }
            if grants.insert(grant.field.clone(), bits).is_some() {
                return Err("field permissions repeat a field grant".into());
            }
        }
        Ok(Self {
            grants,
            disclose_identity: input.disclose_document_identity,
            derived: None,
        })
    }
    pub(crate) fn can_disclose_identity(&self) -> bool {
        self.disclose_identity
    }
    fn granted(&self, field: &str, bit: u8) -> bool {
        if !self.grants.get(field).is_some_and(|bits| bits & bit != 0) {
            return false;
        }
        match self.derived_inputs(field) {
            None => true,
            Some((inputs, reads_stable_key)) => {
                inputs
                    .iter()
                    .all(|input| self.grants.get(input).is_some_and(|bits| bits & bit != 0))
                    && (!reads_stable_key || self.disclose_identity)
            }
        }
    }
    pub(crate) fn can_use(&self, field: &str) -> bool {
        self.granted(field, 1)
    }
    pub(crate) fn can_disclose(&self, field: &str) -> bool {
        self.granted(field, 2)
    }
    fn denied() -> Status {
        Status::permission_denied("field access is not granted")
    }
    fn require_use(&self, field: &str) -> Result<(), Status> {
        if self.can_use(field) {
            Ok(())
        } else {
            Err(Self::denied())
        }
    }
    fn require_disclose(&self, field: &str) -> Result<(), Status> {
        if self.can_disclose(field) {
            Ok(())
        } else {
            Err(Self::denied())
        }
    }
    pub(crate) fn dictionary(&self, field: &str) -> Result<(), Status> {
        self.require_use(field)?;
        self.require_disclose(field)
    }
    pub(crate) fn suggest(&self, req: &SuggestRequest) -> Result<(), Status> {
        let SuggestRequest {
            field,
            collection: _,
            prefix: _,
            limit: _,
            max_scan: _,
            analysis: _,
        } = req;
        self.dictionary(field)
    }
    pub(crate) fn term_suggest(&self, req: &TermSuggestRequest) -> Result<(), Status> {
        let TermSuggestRequest {
            field,
            collection: _,
            text: _,
            analysis: _,
            max_edits: _,
            prefix_length: _,
            limit: _,
            max_scan: _,
            mode: _,
        } = req;
        self.dictionary(field)
    }
}

#[cfg(test)]
mod derived_tests;
