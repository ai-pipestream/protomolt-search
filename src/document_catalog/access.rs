//! Local source access, independent of distributed owner activation.
use super::*;
use crate::authorization::{AccessPermit, PinnedAccess};
use crate::pb::storage::{
    SourceHistorySeal, SourceRetirementIntent, SourceRetirementRequest, SourceSealRequest,
};
use crate::pb::AccessAction;

pub(super) fn validate_binding(binding: &SourceResourceBinding) -> Result<(), Status> {
    if binding.format_version != 1 || binding.workspace.is_empty() {
        return Err(Status::invalid_argument(
            "source resource binding requires format 1 and a workspace",
        ));
    }
    crate::collections::validate_name(&binding.workspace).map_err(Status::invalid_argument)?;
    if !binding.collection.is_empty() {
        crate::collections::validate_name(&binding.collection).map_err(Status::invalid_argument)?;
    }
    Ok(())
}

pub(super) fn authorize<'a>(
    binding: &SourceResourceBinding,
    permit: &'a AccessPermit,
    action: AccessAction,
) -> Result<PinnedAccess<'a>, Status> {
    let pinned = permit.pin()?;
    let decision = pinned.decision();
    if decision.workspace != binding.workspace
        || decision.collection != binding.collection
        || decision.action != action as i32
    {
        return Err(Status::permission_denied(
            "source operation requires the bound workspace, collection and action",
        ));
    }
    Ok(pinned)
}

/// Durable source catalog whose synchronous operations require current scoped
/// permission through commit. Administration does not imply ingest or raw source
/// disclosure. This local resource binding is not exclusive distributed ownership.
/// No inner catalog accessor exists: future retrieval/publication adapters must
/// enforce their own document/field grants and admission protocol.
pub struct AccessControlledCatalog {
    pub(super) inner: DocumentCatalog,
    pub(super) binding: SourceResourceBinding,
    #[cfg(all(any(test, feature = "fault-injection"), feature = "net"))]
    pub(super) bind_fault: Option<super::managed::BindFault>,
}

impl AccessControlledCatalog {
    pub fn create(
        path: &Path,
        binding: &SourceResourceBinding,
        permit: &AccessPermit,
    ) -> Result<Self, Status> {
        Self::open_file(path, binding, permit, true)
    }

    pub fn open(
        path: &Path,
        binding: &SourceResourceBinding,
        permit: &AccessPermit,
    ) -> Result<Self, Status> {
        Self::open_file(path, binding, permit, false)
    }

    fn open_file(
        path: &Path,
        binding: &SourceResourceBinding,
        permit: &AccessPermit,
        create: bool,
    ) -> Result<Self, Status> {
        validate_binding(binding)?;
        let _guard = authorize(binding, permit, AccessAction::Admin)?;
        let inner =
            DocumentCatalog::open_file(path, &binding.collection, create, Some(binding.clone()))?;
        Ok(Self {
            inner,
            binding: binding.clone(),
            #[cfg(all(any(test, feature = "fault-injection"), feature = "net"))]
            bind_fault: None,
        })
    }

    pub fn resource_binding(&self) -> &SourceResourceBinding {
        &self.binding
    }

    /// Discover the pinned local history without disclosing source records.
    pub fn write_target(
        &self,
        permit: &AccessPermit,
    ) -> Result<crate::pb::DocumentWriteTarget, Status> {
        let _guard = authorize(&self.binding, permit, AccessAction::Ingest)?;
        let transaction = self.inner.database.begin_read().map_err(storage)?;
        let metadata = transaction.open_table(META).map_err(storage)?;
        let header: DocumentCatalogHeader = decode_header(
            metadata
                .get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?
                .value(),
        )?;
        validate_current_header(&header)?;
        self.inner.validate_resource_binding(&header)?;
        Ok(crate::pb::DocumentWriteTarget {
            workspace: self.binding.workspace.clone(),
            collection: self.binding.collection.clone(),
            history_id: header.history_id,
        })
    }

    pub fn accept(
        &self,
        permit: &AccessPermit,
        request: &AcceptDocumentRequest,
    ) -> Result<DocumentWriteReceipt, Status> {
        let guard = authorize(&self.binding, permit, AccessAction::Ingest)?;
        self.inner
            .accept_as(request, Some(&guard.decision().principal), None, 0)
            .map(|written| written.receipt)
    }

    /// Attribute one legacy retry decision without changing its receipt or source.
    pub fn assign_legacy_actor(
        &self,
        permit: &AccessPermit,
        request: &crate::pb::storage::SourceActorAssignment,
    ) -> Result<(), Status> {
        let _guard = authorize(&self.binding, permit, AccessAction::Admin)?;
        self.inner.assign_legacy_actor(request)
    }

    pub fn begin_retirement(
        &self,
        permit: &AccessPermit,
        request: &SourceRetirementRequest,
    ) -> Result<SourceRetirementIntent, Status> {
        let _guard = authorize(&self.binding, permit, AccessAction::Admin)?;
        self.inner.begin_retirement(request)
    }

    pub fn seal_history(
        &self,
        permit: &AccessPermit,
        request: &SourceSealRequest,
    ) -> Result<SourceHistorySeal, Status> {
        let _guard = authorize(&self.binding, permit, AccessAction::Admin)?;
        self.inner.seal_history(request)
    }

    pub fn retirement_intent(
        &self,
        permit: &AccessPermit,
    ) -> Result<Option<SourceRetirementIntent>, Status> {
        let _guard = authorize(&self.binding, permit, AccessAction::Admin)?;
        self.inner.retirement_intent()
    }

    pub fn history_seal(&self, permit: &AccessPermit) -> Result<Option<SourceHistorySeal>, Status> {
        let _guard = authorize(&self.binding, permit, AccessAction::Admin)?;
        self.inner.history_seal()
    }
}

#[cfg(test)]
mod tests;
