//! The activated source: recovery under the committed fence, admission,
//! and its admitted reads.
use super::*;

impl ActiveManagedCatalog {
    /// Reopen an activated source under the committed fence it records.
    /// The admission proves current Admin and the owner ACTIVE for this
    /// binding; the file's activation must equal the committed one.
    pub fn recover(
        path: &Path,
        authority: &SourceAuthorityStore,
        admission: &SourceAdmission<'_>,
        binding: &SourceManagedBinding,
    ) -> Result<Self, Status> {
        validate_binding(binding)?;
        require_authority(admission, authority)?;
        let held = admission.activated_owner(binding)?;
        let fence = held.activation.expect("checked activation");
        let key = binding
            .preparation
            .as_ref()
            .expect("validated preparation")
            .key
            .as_ref()
            .expect("validated key");
        let inner = DocumentCatalog::open_file_resolving_binding(
            path,
            &key.collection,
            false,
            Some(SourceResourceBinding {
                format_version: 1,
                workspace: key.workspace.clone(),
                collection: key.collection.clone(),
            }),
            |database| {
                let tx = database.begin_read().map_err(storage)?;
                let table = tx.open_table(META).map_err(storage)?;
                let bytes = table
                    .get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("existing document catalog header missing"))?;
                let header = decode_header(bytes.value())?;
                validate_current_header(&header)?;
                if header.managed_binding.as_ref() != Some(binding) {
                    return Err(Status::failed_precondition(
                        "managed source recovery binding differs",
                    ));
                }
                let activation = header.managed_activation.ok_or_else(|| {
                    Status::failed_precondition(
                        "source has no persisted activation; use the prepared adapter",
                    )
                })?;
                if activation.write_epoch != fence.write_epoch
                    || activation.activated_control_revision != fence.activated_control_revision
                    || activation.ownership_generation != held.ownership_generation
                {
                    return Err(Status::failed_precondition(
                        "persisted activation differs from the committed fence",
                    ));
                }
                Ok((Some(binding.clone()), Some(activation)))
            },
        )?;
        let activation = inner
            .managed_activation
            .clone()
            .expect("recovered activation");
        Ok(Self {
            inner,
            binding: binding.clone(),
            activation,
            authority: authority.clone(),
            #[cfg(any(test, feature = "fault-injection"))]
            settle_pause: std::sync::Mutex::new(None),
        })
    }

    pub fn activation(&self) -> &SourceManagedActivation {
        &self.activation
    }

    /// Fault injection: pause the next write inside its source
    /// transaction, just before its final admission check.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_precommit_pause(&self, pause: std::time::Duration) {
        self.inner.arm_precommit_pause(pause);
    }

    /// Fault injection: pause the next write after its commit returned
    /// durable and before its outcome is judged against the lease.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_postcommit_pause(&self, pause: std::time::Duration) {
        self.inner.arm_postcommit_pause(pause);
    }

    /// Fault injection: pause the next settlement between its freshness
    /// check and its entry check, the gap in which a lease can lapse.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_settle_pause(&self, pause: std::time::Duration) {
        *self.settle_pause.lock().unwrap() = Some(pause);
    }

    pub fn binding(&self) -> &SourceManagedBinding {
        &self.binding
    }

    /// The collection this activated source serves, from its binding.
    pub fn collection(&self) -> &str {
        &self.key().collection
    }

    fn key(&self) -> &crate::pb::storage::LogicalSourceOwner {
        self.binding
            .preparation
            .as_ref()
            .expect("validated preparation")
            .key
            .as_ref()
            .expect("validated key")
    }

    pub(super) fn admit(
        &self,
        admission: &SourceAdmission<'_>,
        action: AccessAction,
    ) -> Result<crate::pb::AccessDecision, Status> {
        require_authority(admission, &self.authority)?;
        admission.admit_write(self.key(), self.activation.write_epoch, action)
    }

    /// Accepted history in commit order under Ingest admission
    /// Accepted history in commit order under Ingest admission, with each
    /// version's write epoch and fence mark.
    pub fn read_accepted(
        &self,
        admission: &SourceAdmission<'_>,
        request: &crate::pb::ReadAcceptedDocumentsRequest,
    ) -> Result<crate::pb::ReadAcceptedDocumentsResponse, Status> {
        self.admit(admission, AccessAction::Ingest)?;
        self.inner.read_accepted(request)
    }

    /// The pinned local history for pinned writes, under Ingest admission.
    pub fn write_target(
        &self,
        admission: &SourceAdmission<'_>,
    ) -> Result<crate::pb::DocumentWriteTarget, Status> {
        self.admit(admission, AccessAction::Ingest)?;
        let key = self.key();
        let transaction = self.inner.database.begin_read().map_err(storage)?;
        let metadata = transaction.open_table(META).map_err(storage)?;
        let header = current_header(&metadata)?;
        self.inner.validate_resource_binding(&header)?;
        Ok(crate::pb::DocumentWriteTarget {
            workspace: key.workspace.clone(),
            collection: key.collection.clone(),
            history_id: header.history_id,
        })
    }

    /// Metadata only, under Admin admission.
    pub fn inspect(
        &self,
        admission: &SourceAdmission<'_>,
        max_metadata_bytes: usize,
    ) -> Result<DocumentCatalogCheckpoint, Status> {
        self.admit(admission, AccessAction::Admin)?;
        let checkpoint = self.inner.capture_checkpoint(max_metadata_bytes)?;
        let header = checkpoint
            .metadata()
            .header
            .as_ref()
            .ok_or_else(|| Status::data_loss("managed source header is missing"))?;
        self.inner.validate_resource_binding(header)?;
        Ok(checkpoint.metadata().clone())
    }
}
