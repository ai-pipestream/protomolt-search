//! Durable managed ownership starts closed. No writable adapter exists here.
use super::*;
use crate::pb::storage::{PreparedSourceOwnerPhase, SourceManagedActivation, SourceManagedBinding};

#[cfg(all(any(test, feature = "fault-injection"), feature = "net"))]
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy)]
pub(super) enum BindFault {
    BeforeCommit,
    AfterCommit,
    ExitBeforeCommit,
    ExitAfterCommit,
}

#[cfg(all(any(test, feature = "fault-injection"), feature = "net"))]
fn inject(fault: Option<BindFault>, after: bool) -> Result<(), Status> {
    match (fault, after) {
        (Some(BindFault::BeforeCommit), false) => Err(Status::aborted(
            "injected source binding BeforeCommit failure",
        )),
        (Some(BindFault::AfterCommit), true) => {
            Err(storage("injected source binding AfterCommit failure"))
        }
        (Some(BindFault::ExitBeforeCommit), false) | (Some(BindFault::ExitAfterCommit), true) => {
            std::process::exit(87)
        }
        _ => Ok(()),
    }
}

/// Read a catalog file's header without opening the catalog: its format
/// with the persisted binding and activation. Recovery presents these
/// exact binding bytes to the committed fence; nothing is reconstructed.
/// A file with no managed binding is not a managed source.
#[cfg(all(feature = "net", feature = "raft"))]
pub(crate) fn header_binding(
    path: &Path,
) -> Result<(u32, SourceManagedBinding, Option<SourceManagedActivation>), Status> {
    if !path.is_file() {
        return Err(Status::not_found(format!(
            "managed catalog {} is missing; restore it rather than creating a fresh authority",
            path.display()
        )));
    }
    let database = Database::open(path).map_err(storage)?;
    let transaction = database.begin_read().map_err(storage)?;
    let metadata = transaction.open_table(META).map_err(storage)?;
    let bytes = metadata
        .get("header")
        .map_err(storage)?
        .ok_or_else(|| Status::data_loss("existing document catalog header missing"))?
        .value()
        .to_vec();
    let header = decode_header(&bytes)?;
    let binding = header.managed_binding.ok_or_else(|| {
        Status::failed_precondition(format!(
            "catalog {} has no managed binding; bind it under a committed preparation first",
            path.display()
        ))
    })?;
    Ok((header.format_version, binding, header.managed_activation))
}

pub(super) fn validate_binding(binding: &SourceManagedBinding) -> Result<(), Status> {
    if binding.format_version != 1 {
        return Err(Status::invalid_argument(
            "managed source binding requires format 1",
        ));
    }
    let authority = binding
        .authority
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("managed source authority identity missing"))?;
    crate::source_owner::identity(authority)?;
    let preparation = binding
        .preparation
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("managed source preparation missing"))?;
    crate::source_owner::owner(preparation).map_err(|e| Status::invalid_argument(e.message()))?;
    if preparation.phase != PreparedSourceOwnerPhase::Prepared as i32 {
        return Err(Status::invalid_argument(
            "managed source binding requires a prepared owner",
        ));
    }
    Ok(())
}

pub(super) fn validate_activation(
    activation: &SourceManagedActivation,
    binding: &SourceManagedBinding,
) -> Result<(), Status> {
    let preparation = binding.preparation.as_ref().expect("validated preparation");
    if activation.format_version != 1
        || activation.authority != binding.authority
        || activation.ownership_generation != preparation.ownership_generation
        || activation.write_epoch == 0
        || activation.activated_control_revision == 0
    {
        return Err(Status::invalid_argument(
            "managed activation requires format 1, the binding's authority and generation, and a nonzero fence",
        ));
    }
    Ok(())
}

pub(super) fn validate_header(header: &DocumentCatalogHeader) -> Result<(), Status> {
    let Some(binding) = &header.managed_binding else {
        return if matches!(
            header.format_version,
            MANAGED_FORMAT | ACTIVE_MANAGED_FORMAT
        ) || header.managed_activation.is_some()
        {
            Err(Status::data_loss(
                "managed catalog owner binding is missing",
            ))
        } else {
            Ok(())
        };
    };
    match (header.format_version, header.managed_activation.as_ref()) {
        (MANAGED_FORMAT, None) => {}
        (ACTIVE_MANAGED_FORMAT, Some(activation)) => {
            validate_binding(binding).map_err(|e| Status::data_loss(e.message()))?;
            validate_activation(activation, binding).map_err(|e| Status::data_loss(e.message()))?;
            if activation.activated_at_sequence > header.accepted_sequence {
                return Err(Status::data_loss(
                    "managed activation sequence is ahead of the accepted sequence",
                ));
            }
        }
        _ => {
            return Err(Status::data_loss(
                "managed source binding requires catalog format 9 without activation or format 11 with it",
            ))
        }
    }
    validate_binding(binding).map_err(|e| Status::data_loss(e.message()))?;
    let prepared = binding.preparation.as_ref().expect("validated preparation");
    let key = prepared.key.as_ref().expect("validated owner key");
    let target = prepared.target.as_ref().expect("validated target");
    let resource = header
        .resource_binding
        .as_ref()
        .ok_or_else(|| Status::data_loss("managed catalog resource binding missing"))?;
    if key.workspace != resource.workspace
        || key.collection != resource.collection
        || target.history_id != header.history_id
        || binding.bound_at_sequence > header.accepted_sequence
    {
        return Err(Status::data_loss(
            "managed source owner, resource, history or accepted sequence differs",
        ));
    }
    Ok(())
}

#[cfg(feature = "net")]
mod adapter {
    use super::*;
    use crate::pb::storage::WriteOutcome;
    use crate::pb::storage::{
        DocumentCatalogCheckpoint, PreparedSourceOwner, SourceAuthorityCommand,
        SourceAuthorityDecision,
    };
    use crate::pb::AccessAction;
    use crate::source_authority::{SourceAdmission, SourceAuthorityStore, VerifiedOwnerCompletion};

    /// A locally bound source retained under its committed preparation. It has
    /// no acceptance, journal mutation, export or inner-catalog accessor. Open
    /// and metadata inspection require current committed administration rights.
    pub struct PreparedManagedCatalog {
        pub(super) inner: DocumentCatalog,
        binding: SourceManagedBinding,
        authority: SourceAuthorityStore,
        #[cfg(any(test, feature = "fault-injection"))]
        pub(super) activate_fault: Option<BindFault>,
    }

    /// A managed source activated under its committed fence. Every write is
    /// admitted through a `SourceAdmission` that finds the owner ACTIVE under
    /// exactly this epoch and the actor permitted, and the admission is held
    /// through the source commit.
    /// What [`ActiveManagedCatalog::accept`] returns: the receipt of a
    /// write whose lease was still open when its commit returned, or the
    /// receipt of one whose lease had lapsed by then, recorded UNCONFIRMED
    /// and awaiting [`ActiveManagedCatalog::settle`].
    #[derive(Clone, Debug, PartialEq)]
    pub enum Acceptance {
        Accepted(crate::pb::DocumentWriteReceipt),
        Unconfirmed(crate::pb::DocumentWriteReceipt),
    }

    impl Acceptance {
        pub fn receipt(&self) -> &crate::pb::DocumentWriteReceipt {
            match self {
                Acceptance::Accepted(receipt) | Acceptance::Unconfirmed(receipt) => receipt,
            }
        }

        /// The receipt whatever the judgement; a caller that uses this
        /// answers for an unconfirmed write itself.
        pub fn into_receipt(self) -> crate::pb::DocumentWriteReceipt {
            match self {
                Acceptance::Accepted(receipt) | Acceptance::Unconfirmed(receipt) => receipt,
            }
        }
    }

    /// What [`ActiveManagedCatalog::settle`] decided: the write is accepted
    /// with its receipt, or fenced with the rejection every retry replays.
    /// An error from `settle` means no decision was made.
    #[derive(Debug)]
    pub enum Settlement {
        Accepted(crate::pb::DocumentWriteReceipt),
        Fenced(Status),
    }

    pub struct ActiveManagedCatalog {
        pub(super) inner: DocumentCatalog,
        binding: SourceManagedBinding,
        activation: SourceManagedActivation,
        authority: SourceAuthorityStore,
        #[cfg(any(test, feature = "fault-injection"))]
        settle_pause: std::sync::Mutex<Option<std::time::Duration>>,
    }

    impl AccessControlledCatalog {
        /// Arm one process-exit fault at the binding's source commit.
        #[cfg(any(test, feature = "fault-injection"))]
        pub fn arm_bind_exit_fault(&mut self, fault: crate::source_authority::ExitFault) {
            self.bind_fault = Some(match fault {
                crate::source_authority::ExitFault::BeforeCommit => BindFault::ExitBeforeCommit,
                crate::source_authority::ExitFault::AfterCommit => BindFault::ExitAfterCommit,
            });
        }

        /// Consume the sole local controlled handle and durably close it under
        /// an exact committed preparation. No source/history bytes are rebuilt.
        /// The one admission resolves current Admin and the pending owner and
        /// stays held through the source commit; no separate policy pin is
        /// taken, so no second authority acquisition can deadlock a waiting
        /// control command. Lock order: admission, source DB writer.
        /// A failed/ambiguous commit drops this handle; existing-state recovery
        /// determines whether the old format or closed managed binding persisted.
        pub fn bind_prepared_owner(
            self,
            admission: &SourceAdmission<'_>,
            authority: &SourceAuthorityStore,
            preparation: &PreparedSourceOwner,
            max_metadata_bytes: usize,
        ) -> Result<PreparedManagedCatalog, Status> {
            if admission.identity() != authority.identity() {
                return Err(crate::raft::reasons::reason(
                    Status::failed_precondition(
                        "admission was acquired from another source authority",
                    ),
                    crate::raft::reasons::ADMISSION_FOREIGN_AUTHORITY,
                ));
            }
            admission.prepared_owner(preparation)?;
            let identity = admission.identity();
            let mut inner = self.inner;
            let binding = (|| -> Result<SourceManagedBinding, Status> {
                // A pending source or maintenance publication must resolve before
                // this adapter closes all mutations. This also bounds inspection.
                let checkpoint = inner.capture_checkpoint(max_metadata_bytes)?;
                let mut tx = inner.writable_transaction()?;
                tx.set_durability(Durability::Immediate).map_err(storage)?;
                let mut header = seal::header_from(&inner, &tx)?;
                if header.format_version != ACCESS_CONTROLLED_FORMAT
                    || header
                        .actor_namespace
                        .as_ref()
                        .is_none_or(|n| n.assigned_operations != n.legacy_operations)
                {
                    return Err(Status::failed_precondition(
                        "managed binding requires complete actor-scoped source history",
                    ));
                }
                let key = preparation.key.as_ref().expect("validated owner key");
                let target = preparation.target.as_ref().expect("validated owner target");
                if key.workspace != self.binding.workspace
                    || key.collection != self.binding.collection
                    || target.history_id != header.history_id
                {
                    return Err(Status::failed_precondition(
                        "committed preparation differs from the source resource or history",
                    ));
                }
                let binding = SourceManagedBinding {
                    format_version: 1,
                    authority: Some(identity.clone()),
                    preparation: Some(preparation.clone()),
                    bound_at_sequence: header.accepted_sequence,
                };
                header.format_version = MANAGED_FORMAT;
                header.managed_binding = Some(binding.clone());
                validate_current_header(&header)?;
                let mut resulting_metadata = checkpoint.metadata().clone();
                resulting_metadata.header = Some(header.clone());
                if resulting_metadata.encoded_len() > max_metadata_bytes {
                    return Err(Status::resource_exhausted(
                        "managed source binding result exceeds metadata budget",
                    ));
                }
                {
                    let mut meta = tx.open_table(META).map_err(storage)?;
                    meta.insert("header", header.encode_to_vec().as_slice())
                        .map_err(storage)?;
                }
                #[cfg(any(test, feature = "fault-injection"))]
                inject(self.bind_fault, false)?;
                tx.commit().map_err(storage)?;
                #[cfg(any(test, feature = "fault-injection"))]
                inject(self.bind_fault, true)?;
                drop(checkpoint);
                Ok(binding)
            })()?;
            inner.managed_binding = Some(binding.clone());
            Ok(PreparedManagedCatalog {
                inner,
                binding,
                authority: authority.clone(),
                #[cfg(any(test, feature = "fault-injection"))]
                activate_fault: None,
            })
        }
    }

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
        pub fn activate(
            self,
            admission: &SourceAdmission<'_>,
        ) -> Result<ActiveManagedCatalog, Status> {
            if admission.identity() != self.authority.identity() {
                return Err(crate::raft::reasons::reason(
                    Status::failed_precondition(
                        "admission was acquired from another source authority",
                    ),
                    crate::raft::reasons::ADMISSION_FOREIGN_AUTHORITY,
                ));
            }
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
            Ok(ActiveManagedCatalog {
                inner,
                binding: self.binding,
                activation,
                authority: self.authority,
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
            if admission.identity() != authority.identity() {
                return Err(crate::raft::reasons::reason(
                    Status::failed_precondition(
                        "admission was acquired from another source authority",
                    ),
                    crate::raft::reasons::ADMISSION_FOREIGN_AUTHORITY,
                ));
            }
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
                    let bytes = table.get("header").map_err(storage)?.ok_or_else(|| {
                        Status::data_loss("existing document catalog header missing")
                    })?;
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

        fn admit(
            &self,
            admission: &SourceAdmission<'_>,
            action: AccessAction,
        ) -> Result<crate::pb::AccessDecision, Status> {
            if admission.identity() != self.authority.identity() {
                return Err(crate::raft::reasons::reason(
                    Status::failed_precondition(
                        "admission was acquired from another source authority",
                    ),
                    crate::raft::reasons::ADMISSION_FOREIGN_AUTHORITY,
                ));
            }
            admission.admit_write(self.key(), self.activation.write_epoch, action)
        }

        /// Accept one write under the admission: current Ingest for the actor
        /// and the owner ACTIVE under this handle's epoch at entry, and the
        /// admission still within its lease at the final check inside the
        /// source transaction, immediately before commit. Between the two
        /// checks the admission's shared guard keeps policy and ownership
        /// from changing on this store; only time passes, and the final check
        /// is where it is measured. Retries resolve from the actor-scoped
        /// record.
        ///
        /// The commit's return is judged against the lease once more
        /// (docs/document-writes.md, "Write outcomes"): a write whose lease
        /// is still open is `Accepted`; one whose lease lapsed while the
        /// commit was becoming durable is recorded UNCONFIRMED and returned
        /// as `Unconfirmed`, for the caller to settle under a fresh lease
        /// with [`settle`](Self::settle). A retry that finds an UNCONFIRMED
        /// record settles it under its own admission, which is fresh at
        /// entry, so the retry answers with the decision.
        pub fn accept(
            &self,
            admission: &SourceAdmission<'_>,
            request: &crate::pb::AcceptDocumentRequest,
        ) -> Result<Acceptance, Status> {
            // A retry of an operation awaiting its decision, or already
            // fenced, is answered from the record before the entry check:
            // the decision is what the retry asks for, and for an actor
            // whose right is gone the decision is the fence, not a
            // permission error that would hide it.
            if let Some(operation) = self
                .inner
                .recorded_operation(Some(admission.principal()), &request.operation_id)?
            {
                let request_sha = crate::sha256::digest(&request.encode_to_vec());
                if operation.request_sha256.as_slice() != request_sha {
                    return Err(crate::raft::reasons::reason(
                        Status::already_exists(
                            "operation_id was used for a different document write",
                        ),
                        crate::raft::reasons::OUTCOME_OPERATION_REUSED,
                    ));
                }
                match outcome::outcome_of(operation.outcome)? {
                    WriteOutcome::Accepted => {}
                    WriteOutcome::Unconfirmed => {
                        return match self.settle_under(
                            admission,
                            admission.principal(),
                            &request.operation_id,
                        )? {
                            Settlement::Accepted(receipt) => {
                                Ok(Acceptance::Accepted(crate::pb::DocumentWriteReceipt {
                                    replayed: true,
                                    ..receipt
                                }))
                            }
                            Settlement::Fenced(rejection) => Err(rejection),
                        };
                    }
                    WriteOutcome::Fenced => {
                        let receipt = operation
                            .receipt
                            .ok_or_else(|| Status::data_loss("operation receipt missing"))?;
                        return Err(outcome::fenced(&receipt, operation.fenced_at_revision));
                    }
                }
            }
            let decision = self.admit(admission, AccessAction::Ingest)?;
            let fence = || admission.check_fresh();
            let written = self.inner.accept_as(
                request,
                Some(&decision.principal),
                Some(&fence),
                self.activation.write_epoch,
            )?;
            match written.outcome {
                WriteOutcome::Unconfirmed => {
                    // The record was marked between this call's first look
                    // and the write's own retry lookup, by the call that wrote
                    // it: settle it here, as the first look would have.
                    match self.settle_under(
                        admission,
                        &decision.principal,
                        &request.operation_id,
                    )? {
                        Settlement::Accepted(receipt) => {
                            Ok(Acceptance::Accepted(crate::pb::DocumentWriteReceipt {
                                replayed: true,
                                ..receipt
                            }))
                        }
                        Settlement::Fenced(rejection) => Err(rejection),
                    }
                }
                WriteOutcome::Fenced => Err(Status::internal(
                    "a fenced record was returned as a receipt",
                )),
                WriteOutcome::Accepted if written.receipt.replayed => {
                    Ok(Acceptance::Accepted(written.receipt))
                }
                WriteOutcome::Accepted => match admission.check_fresh() {
                    Ok(()) => Ok(Acceptance::Accepted(written.receipt)),
                    Err(_) => {
                        self.inner
                            .mark_unconfirmed(Some(&decision.principal), &request.operation_id)?;
                        Ok(Acceptance::Unconfirmed(written.receipt))
                    }
                },
            }
        }

        /// Settle a write returned `Unconfirmed` by [`accept`](Self::accept),
        /// under a fresh leased admission for the same actor: the entry
        /// check run again. Current right: the record becomes ACCEPTED and
        /// the receipt is returned. Right gone (the grant revoked, the owner
        /// no longer ACTIVE under this epoch): the record becomes FENCED at
        /// the store's applied control revision and the settlement names
        /// the durable version; every retry replays that rejection. An
        /// admission that is itself no longer fresh settles nothing and is
        /// an error by name, so the record stays UNCONFIRMED for the next
        /// attempt.
        pub fn settle(
            &self,
            admission: &SourceAdmission<'_>,
            principal: &str,
            operation_id: &[u8],
        ) -> Result<Settlement, Status> {
            if admission.identity() != self.authority.identity() {
                return Err(crate::raft::reasons::reason(
                    Status::failed_precondition(
                        "admission was acquired from another source authority",
                    ),
                    crate::raft::reasons::ADMISSION_FOREIGN_AUTHORITY,
                ));
            }
            if admission.principal() != principal {
                return Err(Status::permission_denied(
                    "a write is settled under an admission for the actor that wrote it",
                ));
            }
            self.settle_under(admission, principal, operation_id)
        }

        fn settle_under(
            &self,
            admission: &SourceAdmission<'_>,
            principal: &str,
            operation_id: &[u8],
        ) -> Result<Settlement, Status> {
            let Some(operation) = self
                .inner
                .recorded_operation(Some(principal), operation_id)?
            else {
                return Err(crate::raft::reasons::reason(
                    Status::not_found("the actor has no operation with this id to settle"),
                    crate::raft::reasons::OUTCOME_NO_OPERATION,
                ));
            };
            let receipt = operation
                .receipt
                .ok_or_else(|| Status::data_loss("operation receipt missing"))?;
            match outcome::outcome_of(operation.outcome)? {
                WriteOutcome::Accepted => return Ok(Settlement::Accepted(receipt)),
                WriteOutcome::Fenced => {
                    return Ok(Settlement::Fenced(outcome::fenced(
                        &receipt,
                        operation.fenced_at_revision,
                    )))
                }
                WriteOutcome::Unconfirmed => {}
            }
            // A lease that lapsed again decides nothing.
            admission.check_fresh()?;
            #[cfg(any(test, feature = "fault-injection"))]
            if let Some(pause) = self.settle_pause.lock().unwrap().take() {
                std::thread::sleep(pause);
            }
            let settled = match self.admit(admission, AccessAction::Ingest) {
                Ok(_) => outcome::Settled::Accepted,
                Err(_) => {
                    // The entry check fails for a lapsed lease as well as for
                    // a right that is gone; only the second is a fence. A
                    // lease that lapsed between the check above and the entry
                    // check decides nothing, and the record stays
                    // unconfirmed for the next attempt.
                    admission.check_fresh()?;
                    outcome::Settled::Fenced {
                        at_revision: admission.applied_control_revision(),
                    }
                }
            };
            match self
                .inner
                .settle_outcome(Some(principal), operation_id, settled)?
            {
                WriteOutcome::Accepted => Ok(Settlement::Accepted(receipt)),
                WriteOutcome::Fenced => {
                    // Fenced here, or fenced by a concurrent settlement whose
                    // revision the record now names.
                    let at_revision = match settled {
                        outcome::Settled::Fenced { at_revision } => at_revision,
                        outcome::Settled::Accepted => self
                            .inner
                            .recorded_operation(Some(principal), operation_id)?
                            .map(|operation| operation.fenced_at_revision)
                            .unwrap_or(0),
                    };
                    Ok(Settlement::Fenced(outcome::fenced(&receipt, at_revision)))
                }
                WriteOutcome::Unconfirmed => {
                    Err(Status::internal("an unconfirmed record was not settled"))
                }
            }
        }

        /// Accepted history in commit order under Ingest admission
        /// (docs/document-writes.md, "Ordered source history"), with each
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
}

#[cfg(feature = "net")]
pub use adapter::{Acceptance, ActiveManagedCatalog, PreparedManagedCatalog, Settlement};

#[cfg(all(test, feature = "net"))]
mod tests;
