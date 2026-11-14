//! Writing the control state, with the faults a test can inject at each step.

use std::path::Path;
#[cfg(test)]
use std::sync::Mutex;

use super::state::StoredState;
use super::storage;

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StateWriteFault {
    BeforeRename,
    AfterRename,
    AfterSync,
}

#[cfg(test)]
fn inject_state_write_fault(
    fault: &Mutex<Option<StateWriteFault>>,
    boundary: StateWriteFault,
) -> Result<(), String> {
    let mut selected = fault.lock().unwrap();
    if *selected == Some(boundary) {
        *selected = None;
        return Err(format!(
            "injected control state write failure at {boundary:?}"
        ));
    }
    Ok(())
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

pub(super) fn write_state(
    path: &Path,
    state: &StoredState,
    #[cfg(test)] fault: &Mutex<Option<StateWriteFault>>,
) -> Result<(), StateWriteError> {
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|error| format!("encode control state: {error}"))?;
    write_bytes(
        path,
        &bytes,
        #[cfg(test)]
        fault,
    )
}

fn write_bytes(
    path: &Path,
    bytes: &[u8],
    #[cfg(test)] fault: &Mutex<Option<StateWriteFault>>,
) -> Result<(), StateWriteError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let candidate = storage::Candidate::write(path, bytes)?;
    #[cfg(test)]
    inject_state_write_fault(fault, StateWriteFault::BeforeRename)?;
    // Once publication has been attempted, a failed syscall must not be
    // interpreted as proof that the old state is still authoritative.
    candidate.publish(path).map_err(|error| {
        StateWriteError::publication(format!("replace {}: {error}", path.display()))
    })?;
    #[cfg(test)]
    inject_state_write_fault(fault, StateWriteFault::AfterRename)
        .map_err(StateWriteError::publication)?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            StateWriteError::publication(format!("sync {}: {error}", parent.display()))
        })?;
    #[cfg(test)]
    inject_state_write_fault(fault, StateWriteFault::AfterSync)
        .map_err(StateWriteError::publication)?;
    Ok(())
}
