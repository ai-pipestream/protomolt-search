//! Process-local authority freshness. A persisted decision is a rollback floor,
//! not proof that a disconnected receiver still has permission to accept writes.

use super::{ReplayAdmissionPolicy, ReplayJournal, Status, AUTHORIZED_FORMAT};
use std::sync::MutexGuard;
use std::time::{Duration, Instant};

/// Upper bound only. The trusted publisher must use the smaller remaining
/// validity of its actual authority proof, measured from before its request.
pub const MAX_AUTHORITY_VALIDITY: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct Freshness {
    generation: u64,
    refresh: u64,
    deadline: Option<Instant>,
    exhausted: bool,
}
impl Freshness {
    fn invalidate(&mut self) -> Result<(), Status> {
        self.deadline = None;
        if self.exhausted {
            return Err(Status::unavailable("replay authority counters exhausted"));
        }
        self.generation = match self.generation.checked_add(1) {
            Some(next) => next,
            None => {
                self.exhausted = true;
                return Err(Status::unavailable(
                    "replay authority session generation exhausted",
                ));
            }
        };
        self.refresh = 0;
        Ok(())
    }
    pub(super) fn check(&self, now: Instant, format: u32) -> Result<(), Status> {
        if self.exhausted
            || format != AUTHORIZED_FORMAT
            || !self.deadline.is_some_and(|deadline| now < deadline)
        {
            return Err(Status::unavailable(
                "replay requires a fresh live control-authority decision",
            ));
        }
        Ok(())
    }
}

/// Owned by a trusted authority subscription, never by the replay sender.
/// Dropping or replacing it fences admission. Session creation alone grants
/// nothing; a completed authority refresh is required.
pub struct ReplayAuthoritySession<'a> {
    journal: &'a ReplayJournal,
    generation: u64,
}

/// One authority request begun before its network round trip. Only the newest
/// request in the current session may publish, within its original deadline.
/// A publisher must verify the response against its authority protocol before
/// calling `publish`; this local handle does not authenticate remote authority.
pub struct ReplayAuthorityRefresh<'a> {
    journal: &'a ReplayJournal,
    generation: u64,
    refresh: u64,
    deadline: Instant,
}
impl ReplayJournal {
    pub(super) fn lock_authority(&self) -> Result<MutexGuard<'_, Freshness>, Status> {
        self.authority
            .lock()
            .map_err(|_| Status::unavailable("replay authority lock poisoned"))
    }
    pub(super) fn authority_now(&self) -> Instant {
        #[cfg(test)]
        {
            (self.authority_clock)()
        }
        #[cfg(not(test))]
        {
            Instant::now()
        }
    }
    /// Replace any old subscription, including its outstanding refresh replies.
    pub fn authority_session(&self) -> Result<ReplayAuthoritySession<'_>, Status> {
        let mut state = self.lock_authority()?;
        state.invalidate()?;
        Ok(ReplayAuthoritySession {
            journal: self,
            generation: state.generation,
        })
    }
    /// Fence immediately on a disconnect, lost authority proof or shutdown.
    /// Old sessions and late replies cannot reopen admission afterwards.
    pub fn suspend_authorization(&self) -> Result<(), Status> {
        self.lock_authority()?.invalidate()
    }
    /// Persist a trusted policy decision and invalidate live permission. This
    /// supports durable revocation/offline installation; it never proves that
    /// an allow decision is fresh. Use a live session's refresh to enable it.
    pub fn publish_authorization(&self, policy: &ReplayAdmissionPolicy) -> Result<(), Status> {
        let mut state = self.lock_authority()?;
        state.invalidate()?;
        self.persist_authorization(policy)
    }
}
impl ReplayAuthoritySession<'_> {
    pub fn begin_refresh(&self, valid_for: Duration) -> Result<ReplayAuthorityRefresh<'_>, Status> {
        if valid_for.is_zero() || valid_for > MAX_AUTHORITY_VALIDITY {
            return Err(Status::invalid_argument(
                "replay authority validity must be positive and at most 30 seconds",
            ));
        }
        // Begin the window before waiting for the journal/admission lock too.
        let deadline = self
            .journal
            .authority_now()
            .checked_add(valid_for)
            .ok_or_else(|| Status::invalid_argument("replay authority deadline overflow"))?;
        let mut state = self.journal.lock_authority()?;
        if state.exhausted || self.generation != state.generation {
            return Err(Status::unavailable(
                "replay authority session was superseded",
            ));
        }
        state.refresh = match state.refresh.checked_add(1) {
            Some(next) => next,
            None => {
                state.deadline = None;
                state.exhausted = true;
                return Err(Status::unavailable(
                    "replay authority refresh sequence exhausted",
                ));
            }
        };
        Ok(ReplayAuthorityRefresh {
            journal: self.journal,
            generation: self.generation,
            refresh: state.refresh,
            deadline,
        })
    }
}
impl Drop for ReplayAuthoritySession<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.journal.lock_authority() {
            if state.generation == self.generation {
                state.deadline = None;
            }
        }
    }
}
impl ReplayAuthorityRefresh<'_> {
    pub fn publish(self, policy: &ReplayAdmissionPolicy) -> Result<(), Status> {
        let mut state = self.journal.lock_authority()?;
        if state.exhausted || self.generation != state.generation || self.refresh != state.refresh {
            return Err(Status::unavailable(
                "replay authority refresh was superseded",
            ));
        }
        // A failed replacement must not leave permission from a former policy.
        state.deadline = None;
        if self.journal.authority_now() >= self.deadline {
            return Err(Status::unavailable(
                "replay authority response arrived after its deadline",
            ));
        }
        self.journal.persist_authorization(policy)?;
        if self.journal.authority_now() >= self.deadline {
            return Err(Status::unavailable(
                "replay authority publication exceeded its deadline",
            ));
        }
        state.deadline = Some(self.deadline);
        Ok(())
    }
}
