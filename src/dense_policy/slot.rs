//! The policy a coordinator serves, replaceable while it serves.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use super::DenseExecutionPolicy;

/// The serving policy and the file it is persisted to. Clones of one
/// coordinator share the slot, so an install is seen by every request that
/// starts after it; `optimize_lock` guards single-flight `OptimizeDensePolicy`
/// sweeps across all clones, and `install` serializes disk persistence with
/// the in-memory swap under one write lock.
#[derive(Debug, Default)]
pub struct PolicySlot {
    current: RwLock<Option<Arc<DenseExecutionPolicy>>>,
    path: Option<PathBuf>,
    optimize_lock: tokio::sync::Mutex<()>,
}

impl PolicySlot {
    pub fn new(policy: Option<DenseExecutionPolicy>, path: Option<PathBuf>) -> Self {
        Self {
            current: RwLock::new(policy.map(Arc::new)),
            path,
            optimize_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn current(&self) -> Option<Arc<DenseExecutionPolicy>> {
        self.current
            .read()
            .expect("dense policy slot poisoned")
            .clone()
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Claim single-flight execution for an `OptimizeDensePolicy` sweep.
    pub fn try_lock_optimize(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        self.optimize_lock.try_lock().ok()
    }

    /// Persist `policy` to the slot's file and swap the serving policy while
    /// holding the slot's write lock so disk and memory can never diverge.
    pub fn install(&self, policy: DenseExecutionPolicy) -> Result<(), String> {
        let path = self.path.as_deref().ok_or(
            "this coordinator has no --dense-execution-policy path to install a policy to",
        )?;
        let mut guard = self.current.write().expect("dense policy slot poisoned");
        policy.save(path)?;
        *guard = Some(Arc::new(policy));
        Ok(())
    }
}
