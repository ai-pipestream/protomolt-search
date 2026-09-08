//! Durable managed ownership starts closed. No writable adapter exists here.
use super::*;
use crate::pb::storage::{PreparedSourceOwnerPhase, SourceManagedBinding};

#[cfg(all(test, feature = "net"))]
#[derive(Clone, Copy)]
pub(super) enum BindFault {
    BeforeCommit,
    AfterCommit,
    ExitBeforeCommit,
    ExitAfterCommit,
}

#[cfg(all(test, feature = "net"))]
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

pub(super) fn validate_header(header: &DocumentCatalogHeader) -> Result<(), Status> {
    let Some(binding) = &header.managed_binding else {
        return if header.format_version == MANAGED_FORMAT {
            Err(Status::data_loss(
                "managed catalog owner binding is missing",
            ))
        } else {
            Ok(())
        };
    };
    if header.format_version != MANAGED_FORMAT {
        return Err(Status::data_loss(
            "managed source binding requires catalog format 9",
        ));
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
    use crate::pb::storage::{
        DocumentCatalogCheckpoint, PreparedSourceOwner, SourceAuthorityCommand,
        SourceAuthorityDecision,
    };
    use crate::source_authority::{SourceAdmission, SourceAuthorityStore, VerifiedOwnerCompletion};

    /// A locally bound source retained under its committed preparation. It has
    /// no acceptance, journal mutation, export or inner-catalog accessor. Open
    /// and metadata inspection require current committed administration rights.
    pub struct PreparedManagedCatalog {
        pub(super) inner: DocumentCatalog,
        binding: SourceManagedBinding,
        authority: SourceAuthorityStore,
    }

    impl AccessControlledCatalog {
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
                return Err(Status::failed_precondition(
                    "admission was acquired from another source authority",
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
                #[cfg(test)]
                inject(self.bind_fault, false)?;
                tx.commit().map_err(storage)?;
                #[cfg(test)]
                inject(self.bind_fault, true)?;
                drop(checkpoint);
                Ok(binding)
            })()?;
            inner.managed_binding = Some(binding.clone());
            Ok(PreparedManagedCatalog {
                inner,
                binding,
                authority: authority.clone(),
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
                        Ok(Some(binding))
                    },
                )
            })?;
            let binding = inner.managed_binding.clone().expect("recovered binding");
            Ok(Self {
                inner,
                binding,
                authority: authority.clone(),
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
}

#[cfg(feature = "net")]
pub use adapter::PreparedManagedCatalog;

#[cfg(all(test, feature = "net"))]
mod tests;
