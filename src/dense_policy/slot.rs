//! The policy a coordinator serves, replaceable while it serves.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use super::DenseExecutionPolicy;

/// The serving policy and the file it is persisted to. Clones of one
/// coordinator share the slot, so an install is seen by every request that
/// starts after it; `optimize_lock` guards single-flight `OptimizeDensePolicy`
/// sweeps across all clones, and `install_lock` serializes background disk
/// persistence with the in-memory `Arc` swap without blocking `current()`.
#[derive(Debug, Default)]
pub struct PolicySlot {
    current: RwLock<Option<Arc<DenseExecutionPolicy>>>,
    path: Option<PathBuf>,
    optimize_lock: tokio::sync::Mutex<()>,
    install_lock: tokio::sync::Mutex<()>,
}

impl PolicySlot {
    pub fn new(policy: Option<DenseExecutionPolicy>, path: Option<PathBuf>) -> Self {
        Self {
            current: RwLock::new(policy.map(Arc::new)),
            path,
            optimize_lock: tokio::sync::Mutex::new(()),
            install_lock: tokio::sync::Mutex::new(()),
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

    /// Persist `policy` to the slot's file on the blocking pool under an async
    /// install mutex, then swap the serving `Arc` under a short `RwLock` write
    /// guard so disk and memory stay ordered without blocking `AUTO` readers.
    pub async fn install(&self, policy: DenseExecutionPolicy) -> Result<(), String> {
        let path = self.path.clone().ok_or(
            "this coordinator has no --dense-execution-policy path to install a policy to",
        )?;
        let _install_guard = self.install_lock.lock().await;
        let policy = Arc::new(policy);
        let to_save = Arc::clone(&policy);
        tokio::task::spawn_blocking(move || to_save.save(&path))
            .await
            .map_err(|error| format!("join dense execution policy save task: {error}"))??;
        *self.current.write().expect("dense policy slot poisoned") = Some(policy);
        Ok(())
    }
}
