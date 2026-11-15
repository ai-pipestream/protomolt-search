//! Writing the control state, with the steps a test can make fail.

use std::path::Path;
#[cfg(test)]
use std::sync::{Arc, Mutex};

use super::state::StoredState;
use super::storage;

/// A step of a state write, in order. A test arms one to fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StateWriteFault {
    BeforeRename,
    AfterRename,
    AfterSync,
}

/// Writes the control state file. Outside tests it holds nothing; in a
/// test it can be armed to fail once at a chosen step.
#[derive(Clone, Default)]
pub(super) struct StateWriter {
    #[cfg(test)]
    armed: Arc<Mutex<Option<StateWriteFault>>>,
}

pub(super) struct StateWriteError {
    pub(super) message: String,
    pub(super) may_have_published: bool,
}

impl From<String> for StateWriteError {
    fn from(message: String) -> Self {
        Self {
            message,
            may_have_published: false,
        }
    }
}

impl StateWriteError {
    fn publication(message: String) -> Self {
        Self {
            message,
            may_have_published: true,
        }
    }
}

pub(super) const UNCERTAIN_CONTROL_STATE: &str =
    "control state persistence outcome is uncertain; close every clone and reopen existing durable state before continuing";

impl StateWriter {
    #[cfg(test)]
    pub(super) fn arm(&self, fault: StateWriteFault) {
        *self.armed.lock().unwrap() = Some(fault);
    }

    /// Passes, except once at `step` when a test armed that step.
    fn reached(&self, step: StateWriteFault) -> Result<(), String> {
        #[cfg(test)]
        {
            let mut armed = self.armed.lock().unwrap();
            if *armed == Some(step) {
                *armed = None;
                return Err(format!("injected control state write failure at {step:?}"));
            }
        }
        let _ = step;
        Ok(())
    }

    pub(super) fn write(&self, path: &Path, state: &StoredState) -> Result<(), StateWriteError> {
        let bytes = serde_json::to_vec_pretty(state)
            .map_err(|error| format!("encode control state: {error}"))?;
        self.write_bytes(path, &bytes)
    }

    fn write_bytes(&self, path: &Path, bytes: &[u8]) -> Result<(), StateWriteError> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let candidate = storage::Candidate::write(path, bytes)?;
        self.reached(StateWriteFault::BeforeRename)?;
        // Once publication has been attempted, a failed syscall must not be
        // interpreted as proof that the old state is still authoritative.
        candidate.publish(path).map_err(|error| {
            StateWriteError::publication(format!("replace {}: {error}", path.display()))
        })?;
        self.reached(StateWriteFault::AfterRename)
            .map_err(StateWriteError::publication)?;
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                StateWriteError::publication(format!("sync {}: {error}", parent.display()))
            })?;
        self.reached(StateWriteFault::AfterSync)
            .map_err(StateWriteError::publication)?;
        Ok(())
    }
}
