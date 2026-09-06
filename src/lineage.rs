//! Explicit projection of candidate lineage keys. Row locators and fallback
//! self-parent tags remain generation-local, not stable document identity.
use crate::pb::{LineageField, ResolvedParent};
use tonic::Status;

#[derive(Clone, Debug)]
pub(crate) struct LineageSelection(Vec<LineageField>);
impl LineageSelection {
    pub(crate) fn new(fields: &[i32]) -> Result<Self, Status> {
        if fields.is_empty() {
            return Ok(Self(vec![LineageField::ParentId, LineageField::GroupId]));
        }
        let mut selected = Vec::new();
        for field in fields {
            let field = match LineageField::try_from(*field) {
                Ok(field @ (LineageField::ParentId | LineageField::GroupId)) => field,
                _ => return Err(Status::invalid_argument("unknown lineage field")),
            };
            if selected.contains(&field) {
                return Err(Status::invalid_argument("duplicate lineage field"));
            }
            selected.push(field);
        }
        selected.sort_by_key(|field| *field as i32);
        Ok(Self(selected))
    }
    pub(crate) fn wire(&self) -> Vec<i32> {
        self.0.iter().map(|field| *field as i32).collect()
    }
    pub(crate) fn contains(&self, field: LineageField) -> bool {
        self.0.contains(&field)
    }
    pub(crate) fn columns(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.0.iter().map(|field| match field {
            LineageField::ParentId => "parent_id",
            LineageField::GroupId => "group_id",
            _ => unreachable!("validated selection"),
        })
    }
    pub(crate) fn validate_echo(&self, fields: &[i32]) -> Result<(), Status> {
        if fields != self.wire() {
            return Err(Status::failed_precondition(
                "lineage response omitted or changed its field selection",
            ));
        }
        Ok(())
    }
    pub(crate) fn validate_row(&self, row: &ResolvedParent) -> Result<(), Status> {
        if (!self.contains(LineageField::ParentId) && row.parent_id != 0)
            || (!self.contains(LineageField::GroupId) && row.group_id != 0)
        {
            return Err(Status::failed_precondition(
                "lineage response disclosed an unrequested field",
            ));
        }
        Ok(())
    }
}
