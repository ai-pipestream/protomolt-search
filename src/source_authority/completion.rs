//! Readiness confirmation: the verified completion a hosting adapter holds
//! and the admission checks a `ConfirmReady` command passes with it.
use super::*;

/// How a control command reached the store. Admission happens once, at
/// proposal; application of a committed command performs no holder check.
pub(super) enum OwnerAdmission<'a> {
    /// The general command path: no holder, so no readiness confirmation.
    None,
    /// The hosting adapter's held binding, checked before the transition.
    Holder(&'a VerifiedOwnerCompletion),
    /// A command from the committed log, replayed as committed evidence.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    Committed,
}

/// The verified completion a hosting adapter derives from the managed
/// binding it holds under its exclusive source lock. Only that adapter can
/// construct it; readiness is never confirmed from caller bytes.
pub struct VerifiedOwnerCompletion {
    pub(super) binding: SourceManagedBinding,
    pub(super) completion: SourceOwnerCompletion,
}

impl VerifiedOwnerCompletion {
    pub(crate) fn from_binding(binding: &SourceManagedBinding) -> Result<Self, Status> {
        let preparation = binding
            .preparation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("managed binding has no preparation"))?;
        contract::owner(preparation).map_err(|e| Status::invalid_argument(e.message()))?;
        let target = preparation.target.as_ref().expect("validated target");
        Ok(Self {
            binding: binding.clone(),
            completion: SourceOwnerCompletion {
                format_version: 1,
                binding_sha256: turbo_sha256::sha256(&binding.encode_to_vec()).0.to_vec(),
                bound_at_sequence: binding.bound_at_sequence,
                history_id: target.history_id.clone(),
                node_id: target.node_id.clone(),
                storage_incarnation: target.storage_incarnation.clone(),
            },
        })
    }

    /// The completion the confirmation command must carry.
    pub fn completion(&self) -> &SourceOwnerCompletion {
        &self.completion
    }
}

impl SourceAuthorityStore {
    /// PREPARED -> READY under the hosting adapter's verified completion. The
    /// general `execute` path refuses this action.
    pub fn confirm_owner_ready(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
        verified: &VerifiedOwnerCompletion,
    ) -> Result<SourceAuthorityDecision, Status> {
        self.direct()?;
        if !matches!(command.action, Some(Action::ConfirmReady(_))) {
            return Err(Status::invalid_argument(
                "confirm_owner_ready takes a ConfirmReady command",
            ));
        }
        if verified.binding.authority.as_ref() != Some(&self.inner.identity) {
            return Err(Status::failed_precondition(
                "managed binding names another source authority",
            ));
        }
        self.execute_admitted(principal, command, OwnerAdmission::Holder(verified))
    }

    /// Proposal admission of a readiness confirmation, the same checks the
    /// direct path performs before its transition, as a read.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    pub(crate) fn proposal_admits_readiness(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
        verified: &VerifiedOwnerCompletion,
    ) -> Result<(), Status> {
        let Some(Action::ConfirmReady(request)) = command.action.as_ref() else {
            return Err(Status::invalid_argument(
                "proposal_admits_readiness takes a ConfirmReady command",
            ));
        };
        if verified.binding.authority.as_ref() != Some(&self.inner.identity) {
            return Err(Status::failed_precondition(
                "managed binding names another source authority",
            ));
        }
        let key = command
            .key
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("source authority command key is required"))?;
        contract::key(key, true)?;
        contract::principal(principal)?;
        self.guarded(|| {
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let owners = tx.open_table(OWNERS).map_err(storage)?;
            let before: Option<PreparedSourceOwner> = owners
                .get(key.encode_to_vec().as_slice())
                .map_err(storage)?
                .map(|v| contract::decode(v.value()))
                .transpose()?;
            if verified.binding.preparation.as_ref() != before.as_ref() {
                return Err(Status::failed_precondition(
                    "managed binding preparation differs from the committed owner",
                ));
            }
            if request.completion.as_ref() != Some(&verified.completion) {
                return Err(Status::permission_denied(
                    "readiness confirmation differs from the held binding's completion",
                ));
            }
            Ok(())
        })
    }
}
