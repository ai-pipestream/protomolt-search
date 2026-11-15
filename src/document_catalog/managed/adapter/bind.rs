//! Close a controlled source under an exact committed preparation.
use super::*;

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
        require_authority(admission, authority)?;
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
