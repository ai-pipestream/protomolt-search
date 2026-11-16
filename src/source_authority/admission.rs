//! Admission of source work against committed policy and ownership: the
//! shared fence, its Raft lease and the checks a source write passes.
use super::*;

/// The validity interval of a Raft-hosted admission.
/// `anchor` is taken before the host's read barrier is invoked, so it is at
/// or before the instant the barrier's heartbeats were sent; every follower
/// that acknowledged them refreshed its own leader lease at a later instant,
/// and none grants a vote before that instant plus the election ceiling.
/// The interval therefore includes every response, catch-up and scheduling
/// delay between the barrier and the grant. The wall-clock anchor catches a
/// suspend the monotonic clock does not count.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionLease {
    pub anchor: std::time::Instant,
    pub anchor_wall: std::time::SystemTime,
    pub ttl: std::time::Duration,
}

/// An owned admission lease granted by the Raft host: the store handle, the
/// actor and the interval anchored before the host's read barrier. Turning
/// it into a `SourceAdmission` takes the shared guard at the grant, so a
/// value that is never turned into an admission blocks no control command.
/// The grant measures the interval from the anchor: a grant taken later on
/// another thread is the "paused between barrier and grant" case, and the
/// delay consumes the interval without extending it. `Send + 'static`, so
/// the grant and the source transaction can run on a blocking worker.
pub struct LeasedAdmission {
    store: SourceAuthorityStore,
    principal: String,
    lease: AdmissionLease,
}

impl LeasedAdmission {
    #[cfg(feature = "raft")]
    pub(crate) fn new(store: SourceAuthorityStore, principal: &str, lease: AdmissionLease) -> Self {
        Self {
            store,
            principal: principal.to_string(),
            lease,
        }
    }

    /// The grant. Rejected by name when the interval elapsed before it.
    pub fn admission(&self) -> Result<SourceAdmission<'_>, Status> {
        self.store.leased_admission(&self.principal, self.lease)
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }
}

impl AdmissionLease {
    pub fn deadline(&self) -> std::time::Instant {
        self.anchor + self.ttl
    }

    /// Expired on the monotonic clock, expired on the wall clock (a
    /// suspended process or machine), or a wall clock that moved backwards:
    /// each is a named refusal, never an extension.
    pub fn fresh(&self) -> Result<(), Status> {
        if std::time::Instant::now() >= self.deadline() {
            return Err(crate::raft::reasons::reason(
                Status::failed_precondition(
                    "admission lease expired; obtain a fresh admission through the host",
                ),
                crate::raft::reasons::LEASE_INTERVAL_ELAPSED,
            ));
        }
        match std::time::SystemTime::now().duration_since(self.anchor_wall) {
            Ok(elapsed) if elapsed < self.ttl => Ok(()),
            Ok(_) => Err(crate::raft::reasons::reason(
                Status::failed_precondition(
                    "admission lease expired on the wall clock (the process or machine was suspended); obtain a fresh admission through the host",
                ),
                crate::raft::reasons::LEASE_INTERVAL_ELAPSED,
            )),
            Err(_) => Err(crate::raft::reasons::reason(
                Status::failed_precondition(
                    "wall clock moved backwards under an admission lease; obtain a fresh admission through the host",
                ),
                crate::raft::reasons::LEASE_INTERVAL_ELAPSED,
            )),
        }
    }
}

/// One admission acquisition: current committed policy and ownership resolved
/// under a guard that every control command waits for. Hold it through the
/// source commit it admits; never acquire a second one, pin a permit, or run
/// a control command while holding it — an exclusive waiter would then block
/// the second acquisition behind this one.
#[must_use = "dropping the admission releases the fence"]
pub struct SourceAdmission<'a> {
    store: &'a SourceAuthorityStore,
    principal: String,
    // A Raft-hosted admission is a lease: the host granted it after a
    // linearizable read anchored before the barrier, and it admits nothing
    // past the anchor plus the lease.
    lease: Option<AdmissionLease>,
    // A recovery admission on a hosted store: the applied view, taken at
    // start to open a managed handle. It proves the actor's Admin and the
    // owner ACTIVE as this replica has applied them, which opens a handle
    // and admits no write; every write on the handle takes its own leased
    // admission, and a handle opened on a stale view is rejected there
    // ("the fence has moved").
    recovery: bool,
    _shared: RwLockReadGuard<'a, ()>,
}

impl SourceAdmission<'_> {
    pub fn identity(&self) -> &SourceAuthorityIdentity {
        &self.store.inner.identity
    }

    /// A leased admission past its interval admits nothing; the lease is
    /// what keeps admission authoritative under leader isolation. A
    /// recovery admission is not fresh for a write at all: it opens a
    /// handle and admits nothing.
    fn fresh(&self) -> Result<(), Status> {
        if self.recovery {
            return Err(Status::failed_precondition(
                "a recovery admission opens a managed handle and admits no write; obtain a leased admission through the host",
            ));
        }
        match &self.lease {
            Some(lease) => lease.fresh(),
            None => Ok(()),
        }
    }

    /// The check a recovery admission passes: the applied view of this
    /// replica, enough to open a handle whose every write is admitted
    /// again under a lease.
    fn applied(&self) -> Result<(), Status> {
        if self.recovery {
            return Ok(());
        }
        self.fresh()
    }

    /// The final check a source write performs inside its own transaction,
    /// immediately before commit: the boundary at which the write becomes
    /// authorized.
    pub fn check_fresh(&self) -> Result<(), Status> {
        self.fresh()
    }

    pub fn lease(&self) -> Option<&AdmissionLease> {
        self.lease.as_ref()
    }

    pub fn expires_at(&self) -> Option<std::time::Instant> {
        self.lease.as_ref().map(AdmissionLease::deadline)
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// The control revision this replica has applied, as the admission
    /// sees it: the revision a fenced write is marked with.
    pub fn applied_control_revision(&self) -> u64 {
        *self.store.inner.applied.borrow()
    }

    /// The current committed decision for this actor on one collection.
    pub fn authorize(
        &self,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        self.fresh()?;
        self.store.authorize(&self.principal, collection, action)
    }

    /// The committed owner row for `key`; `disclose` requires current Admin.
    fn owner_row(
        &self,
        key: &LogicalSourceOwner,
        disclose: bool,
    ) -> Result<PreparedSourceOwner, Status> {
        self.store.guarded(|| {
            let tx = self.store.inner.database().begin_read().map_err(storage)?;
            if disclose {
                self.store.read_policy(&tx, &self.principal, key)?;
            }
            committed_preparation(&tx, key)
        })
    }

    /// Current Admin and the owner ACTIVE for exactly this managed binding:
    /// same key, target, workflow and generation as its preparation, and a
    /// readiness completion derived from these binding bytes. Returns the row
    /// with its committed fence, for the owner to persist before it admits
    /// any write.
    pub fn activated_owner(
        &self,
        binding: &SourceManagedBinding,
    ) -> Result<PreparedSourceOwner, Status> {
        self.applied()?;
        let verified = VerifiedOwnerCompletion::from_binding(binding)?;
        if binding.authority.as_ref() != Some(&self.store.inner.identity) {
            return Err(Status::failed_precondition(
                "managed binding names another source authority",
            ));
        }
        let ready = binding.preparation.as_ref().expect("validated preparation");
        let key = ready.key.as_ref().expect("validated key");
        let held = self.owner_row(key, true)?;
        if held.phase != PreparedSourceOwnerPhase::Active as i32
            || held.target != ready.target
            || held.workflow_id != ready.workflow_id
            || held.ownership_generation != ready.ownership_generation
            || held.readiness.as_ref().and_then(|r| r.completion.as_ref())
                != Some(&verified.completion)
            || held.activation.is_none()
        {
            return Err(Status::failed_precondition(
                "source owner is not ACTIVE for this binding; READY alone admits no write",
            ));
        }
        Ok(held)
    }

    /// Admission of one source write: current permission for `action` on the
    /// owner's collection and the owner ACTIVE under exactly `write_epoch`.
    /// Held through the source commit, so no activation change can be missed.
    pub fn admit_write(
        &self,
        key: &LogicalSourceOwner,
        write_epoch: u64,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        self.fresh()?;
        contract::key(key, true)?;
        let decision = self.authorize(&key.collection, action)?;
        if decision.workspace != key.workspace {
            return Err(Status::permission_denied(
                "source write permission names another workspace",
            ));
        }
        let held = self.owner_row(key, false)?;
        if held.phase != PreparedSourceOwnerPhase::Active as i32
            || held.activation.as_ref().map(|a| a.write_epoch) != Some(write_epoch)
        {
            return Err(crate::raft::reasons::reason(
                Status::failed_precondition(
                    "source owner is not ACTIVE under this write epoch; the fence has moved",
                ),
                crate::raft::reasons::ADMISSION_EPOCH_MOVED,
            ));
        }
        Ok(decision)
    }

    /// Current Admin on the owner's collection and the exact pending
    /// preparation. Owner-side work runs under this admission through its
    /// source commit; no command can change the owner or the policy meanwhile.
    pub fn prepared_owner(&self, expected: &PreparedSourceOwner) -> Result<(), Status> {
        self.fresh()?;
        contract::owner(expected).map_err(|e| Status::invalid_argument(e.message()))?;
        self.store.guarded(|| {
            let key = expected
                .key
                .as_ref()
                .ok_or_else(|| missing("prepared owner key"))?;
            let tx = self.store.inner.database().begin_read().map_err(storage)?;
            self.store.read_policy(&tx, &self.principal, key)?;
            let held = committed_preparation(&tx, key)?;
            if &held != expected || held.phase != PreparedSourceOwnerPhase::Prepared as i32 {
                return Err(Status::failed_precondition(
                    "source owner preparation changed or is no longer pending",
                ));
            }
            Ok(())
        })
    }
}

impl SourceAuthorityStore {
    /// Acquire shared admission for one actor. The actor must be a valid
    /// principal; rights are resolved per collection by the admission's own
    /// methods, so a revoked actor holds a fence that admits nothing.
    pub fn admission(&self, principal: &str) -> Result<SourceAdmission<'_>, Status> {
        if self.raft_hosted() {
            return Err(Status::failed_precondition(
                "source authority is Raft-hosted; a local admission is not authoritative, obtain a leased admission through the host",
            ));
        }
        self.admission_with(principal, None)
    }

    /// A leased admission granted by the Raft host after a linearizable
    /// read; it admits nothing past the lease's interval, and is not
    /// granted at all when that interval has already elapsed.
    pub(crate) fn leased_admission(
        &self,
        principal: &str,
        lease: AdmissionLease,
    ) -> Result<SourceAdmission<'_>, Status> {
        lease.fresh().map_err(|e| {
            crate::raft::reasons::reason(
                Status::failed_precondition(format!(
                    "admission interval elapsed before the grant: {}",
                    e.message()
                )),
                crate::raft::reasons::LEASE_INTERVAL_ELAPSED,
            )
        })?;
        self.admission_with(principal, Some(lease))
    }

    fn admission_with(
        &self,
        principal: &str,
        lease: Option<AdmissionLease>,
    ) -> Result<SourceAdmission<'_>, Status> {
        contract::principal(principal)?;
        let shared = self
            .inner
            .admission
            .read()
            .map_err(|_| storage("admission lock poisoned"))?;
        Ok(SourceAdmission {
            store: self,
            principal: principal.to_string(),
            lease,
            recovery: false,
            _shared: shared,
        })
    }

    /// The applied-view admission that opens a managed handle at start on a
    /// hosted store. It proves the actor's current Admin and the owner
    /// ACTIVE as this replica has applied them, which is enough to open the
    /// handle and,
    /// by name, not enough to admit a write: every write on the handle
    /// takes its own leased admission, so a handle opened on a view the
    /// quorum has since moved past is rejected at its first write. A
    /// member therefore recovers its catalogs whether or not it leads, and
    /// before any leader exists.
    #[cfg(feature = "raft")]
    pub(crate) fn recovery_admission(
        &self,
        principal: &str,
    ) -> Result<SourceAdmission<'_>, Status> {
        let mut admission = self.admission_with(principal, None)?;
        admission.recovery = true;
        Ok(admission)
    }
}

/// The committed, valid preparation recorded for `key`.
fn committed_preparation(
    tx: &redb::ReadTransaction,
    key: &LogicalSourceOwner,
) -> Result<PreparedSourceOwner, Status> {
    let table = tx.open_table(OWNERS).map_err(storage)?;
    let bytes = key.encode_to_vec();
    let held: PreparedSourceOwner = contract::decode(
        table
            .get(bytes.as_slice())
            .map_err(storage)?
            .ok_or_else(|| {
                crate::raft::reasons::reason(
                    Status::failed_precondition("source owner has no committed preparation"),
                    crate::raft::reasons::ADMISSION_UNRECORDED_ACTIVATION,
                )
            })?
            .value(),
    )?;
    contract::owner(&held).map_err(corrupt)?;
    Ok(held)
}
