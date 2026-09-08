//! Destination policy fence for retiring the legacy file authority.
use super::*;
use crate::control_plane::{retirement, DurableControlPlane, RetiredLegacyControl};

impl SourceAuthorityStore {
    /// Retire the exact legacy checkpoint under current resource Admin rights.
    /// This writes only the legacy file; no import, owner or revision is created
    /// in this destination store. The hosting adapter authenticates `principal`.
    pub fn retire_legacy_control(
        &self,
        principal: &str,
        request: &LegacyControlRetirementRequest,
        legacy: &DurableControlPlane,
    ) -> Result<RetiredLegacyControl, Status> {
        retirement::validate_request(request)?;
        self.guarded(|| {
            let key = request.key.as_ref().expect("validated resource");
            let tx = self.inner.database.begin_read().map_err(storage)?;
            let policy = self.read_policy(&tx, principal, key)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let header: SourceAuthorityHeader = contract::decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| missing("header"))?
                    .value(),
            )?;
            let operation = contract::operation_key(principal, key, &request.command_id);
            // Current policy remains locked through the legacy commit. Legacy
            // I/O failures belong to that store and must not latch this one.
            Ok(
                legacy.retire_for_import(&self.inner.identity, &operation, request, || {
                    if header.control_revision != request.expected_control_revision
                        || policy.revision != request.expected_policy_revision
                    {
                        return Err(Status::failed_precondition(
                            "legacy retirement destination control or policy revision changed",
                        ));
                    }
                    Ok(())
                }),
            )
        })?
    }

    /// Recover the same retirement after all legacy clones and holders close.
    /// Current Admin rights are required even for an exact durable retry. The
    /// original revision preconditions are historical, not a new mutation CAS.
    pub fn recover_legacy_retirement(
        &self,
        principal: &str,
        request: &LegacyControlRetirementRequest,
        path: &Path,
    ) -> Result<RetiredLegacyControl, Status> {
        retirement::validate_request(request)?;
        self.guarded(|| {
            let key = request.key.as_ref().expect("validated resource");
            let tx = self.inner.database.begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let operation = contract::operation_key(principal, key, &request.command_id);
            Ok(retirement::recover(
                path,
                &self.inner.identity,
                &operation,
                request,
            ))
        })?
    }
}
