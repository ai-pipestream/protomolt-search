//! The bound, unactivated source: recovery, inspection and activation.
use super::*;

impl PreparedManagedCatalog {
    /// Read-only recovery of an exact existing managed binding. A cancelled
    /// control workflow can still be inspected by a current administrator;
    /// neither cancellation nor this open makes the source writable.
    pub fn open(
        path: &Path,
        authority: &SourceAuthorityStore,
        principal: &str,
        binding: &SourceManagedBinding,
    ) -> Result<Self, Status> {
        validate_binding(binding)?;
        let identity = binding.authority.as_ref().expect("validated identity");
        let key = binding
            .preparation
            .as_ref()
            .expect("validated preparation")
            .key
            .as_ref()
            .expect("validated key");
        let inner = authority.with_resource_admin(principal, identity, key, || {
            DocumentCatalog::open_bound_file(
                path,
                &key.collection,
                false,
                Some(SourceResourceBinding {
                    format_version: 1,
                    workspace: key.workspace.clone(),
                    collection: key.collection.clone(),
                }),
                Some(binding.clone()),
            )
        })?;
        Ok(Self {
            inner,
            binding: binding.clone(),
            authority: authority.clone(),
            #[cfg(any(test, feature = "fault-injection"))]
            activate_fault: None,
        })
    }

    /// Recover a committed binding after a lost response without guessing
    /// the source-selected accepted sequence. This discovers only the
    /// watermark; authority and complete preparation must match exactly.
    pub fn recover(
        path: &Path,
        authority: &SourceAuthorityStore,
        principal: &str,
        identity: &crate::pb::storage::SourceAuthorityIdentity,
        preparation: &PreparedSourceOwner,
    ) -> Result<Self, Status> {
        let expected = SourceManagedBinding {
            format_version: 1,
            authority: Some(identity.clone()),
            preparation: Some(preparation.clone()),
            bound_at_sequence: 0,
        };
        validate_binding(&expected)?;
        let key = preparation.key.as_ref().expect("validated owner key");
        let inner = authority.with_resource_admin(principal, identity, key, || {
            DocumentCatalog::open_file_resolving_binding(
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
                    let bytes = table.get("header").map_err(storage)?.ok_or_else(|| {
                        Status::data_loss("existing document catalog header missing")
                    })?;
                    let header = decode_header(bytes.value())?;
                    validate_current_header(&header)?;
                    let binding = header.managed_binding.ok_or_else(|| {
                        Status::failed_precondition("source has no committed managed binding")
                    })?;
                    if binding.authority.as_ref() != Some(identity)
                        || binding.preparation.as_ref() != Some(preparation)
                    {
                        return Err(Status::failed_precondition(
                            "managed source recovery authority or preparation differs",
                        ));
                    }
                    Ok((Some(binding), None))
                },
            )
        })?;
        let binding = inner.managed_binding.clone().expect("recovered binding");
        Ok(Self {
            inner,
            binding,
            authority: authority.clone(),
            #[cfg(any(test, feature = "fault-injection"))]
            activate_fault: None,
        })
    }

    /// Arm one process-exit fault at the activation's source commit.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_activate_exit_fault(&mut self, fault: crate::source_authority::ExitFault) {
        self.activate_fault = Some(match fault {
            crate::source_authority::ExitFault::BeforeCommit => BindFault::ExitBeforeCommit,
            crate::source_authority::ExitFault::AfterCommit => BindFault::ExitAfterCommit,
        });
    }

    /// Persist the committed fence and open the source for admitted
    /// writes. Requires the owner ACTIVE in control for exactly this
    /// binding, resolved by the one admission that stays held through the
    /// source commit. READY alone never reaches here. Failure before the
    /// source commit leaves format 9; after it, format 10; both recover
    /// from the exact existing file.
    pub fn activate(self, admission: &SourceAdmission<'_>) -> Result<ActiveManagedCatalog, Status> {
        require_authority(admission, &self.authority)?;
        let held = admission.activated_owner(&self.binding)?;
        let fence = held.activation.as_ref().expect("checked activation");
        let mut inner = self.inner;
        let activation = (|| -> Result<SourceManagedActivation, Status> {
            let mut tx = inner.database.begin_write().map_err(storage)?;
            tx.set_durability(Durability::Immediate).map_err(storage)?;
            let mut header = {
                let meta = tx.open_table(META).map_err(storage)?;
                let bytes = meta
                    .get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("catalog header missing"))?
                    .value()
                    .to_vec();
                decode_header(&bytes)?
            };
            validate_current_header(&header)?;
            inner.validate_resource_binding(&header)?;
            if header.format_version != MANAGED_FORMAT
                || header.managed_binding.as_ref() != Some(&self.binding)
            {
                return Err(Status::failed_precondition(
                    "activation requires the exact bound, unactivated managed source",
                ));
            }
            let activation = SourceManagedActivation {
                format_version: 1,
                authority: self.binding.authority.clone(),
                ownership_generation: held.ownership_generation,
                write_epoch: fence.write_epoch,
                activated_control_revision: fence.activated_control_revision,
                activated_at_sequence: header.accepted_sequence,
            };
            validate_activation(&activation, &self.binding)?;
            header.format_version = ACTIVE_MANAGED_FORMAT;
            header.managed_activation = Some(activation.clone());
            validate_current_header(&header)?;
            {
                let mut meta = tx.open_table(META).map_err(storage)?;
                meta.insert("header", header.encode_to_vec().as_slice())
                    .map_err(storage)?;
            }
            #[cfg(any(test, feature = "fault-injection"))]
            inject(self.activate_fault, false)?;
            tx.commit().map_err(storage)?;
            #[cfg(any(test, feature = "fault-injection"))]
            inject(self.activate_fault, true)?;
            Ok(activation)
        })()?;
        inner.managed_activation = Some(activation.clone());
        let unconfirmed = ActiveManagedCatalog::unconfirmed_present(&inner)?;
        Ok(ActiveManagedCatalog {
            inner: Arc::new(inner),
            binding: self.binding,
            activation,
            authority: self.authority,
            judging: std::sync::RwLock::new(()),
            unconfirmed: UnconfirmedSignal::new(unconfirmed),
            #[cfg(any(test, feature = "fault-injection"))]
            settle_pause: std::sync::Mutex::new(None),
        })
    }

    /// The completion fact this adapter vouches for: derived from the exact
    /// binding it validated against the file it holds exclusively.
    pub fn completion(&self) -> Result<VerifiedOwnerCompletion, Status> {
        VerifiedOwnerCompletion::from_binding(&self.binding)
    }

    /// PREPARED -> READY in the control store under this adapter's held
    /// binding. The command's completion must equal `completion()`; the
    /// decision is actor-scoped and retryable like every control command.
    pub fn confirm_ready(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
    ) -> Result<SourceAuthorityDecision, Status> {
        self.authority
            .confirm_owner_ready(principal, command, &self.completion()?)
    }

    pub fn binding(&self, principal: &str) -> Result<SourceManagedBinding, Status> {
        let snapshot = self.inspect(principal, 64 << 20)?;
        snapshot
            .header
            .and_then(|h| h.managed_binding)
            .ok_or_else(|| Status::data_loss("managed source binding is missing"))
    }

    /// Metadata only; does not return source records or a copyable checkpoint.
    pub fn inspect(
        &self,
        principal: &str,
        max_metadata_bytes: usize,
    ) -> Result<DocumentCatalogCheckpoint, Status> {
        let identity = self.binding.authority.as_ref().expect("validated identity");
        let key = self
            .binding
            .preparation
            .as_ref()
            .expect("validated preparation")
            .key
            .as_ref()
            .expect("validated key");
        self.authority
            .with_resource_admin(principal, identity, key, || {
                let checkpoint = self.inner.capture_checkpoint(max_metadata_bytes)?;
                let header = checkpoint
                    .metadata()
                    .header
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("managed source header is missing"))?;
                self.inner.validate_resource_binding(header)?;
                Ok(checkpoint.metadata().clone())
            })
    }
}
