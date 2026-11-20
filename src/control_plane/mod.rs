//! Durable lease, placement, and topology authority.
//!
//! Nodes report capacity and immutable shard generations. Reconciliation is
//! deterministic and idempotent: it expires leases, promotes ready replicas,
//! creates copy/drain/rebalance work, and schedules split, merge, or compaction
//! when the configured thresholds are crossed. Data movement stays on the
//! node-to-node WAL/snapshot paths; this module owns decisions, validated
//! action completion, and complete topology publication.

mod actions;
mod balance;
mod membership;
#[cfg(test)]
mod ownership_tests;
#[cfg(test)]
mod persistence_tests;
mod plan;
mod reconcile;
mod service;
mod state;
#[cfg(test)]
mod state_json_tests;
mod state_write;
mod storage;
#[cfg(test)]
mod tests;

pub use self::balance::{
    BalancePool, BALANCE_DEFAULT_MAX_MOVES, BALANCE_DEFAULT_MAX_RATE_AGE_MS,
    BALANCE_DEFAULT_MIN_GAIN,
};
pub use self::service::ClusterControlService;
#[cfg(test)]
pub(crate) use self::state_write::StateWriteFault;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tonic::{Request, Status};

use crate::coordinator::TopologyRoute;

use self::state::{StoredRoute, StoredState, StoredTopology};
use self::state_write::{StateWriter, UNCERTAIN_CONTROL_STATE};

#[derive(Debug, Clone)]
pub struct ControlPolicy {
    pub lease_ms: u64,
    /// Total ready copies, including the primary.
    pub replication_factor: usize,
    pub split_rows: u64,
    pub merge_rows: u64,
    pub compact_segments: u32,
    pub compact_tombstone_ppm: u32,
    pub history_limit: usize,
}

impl Default for ControlPolicy {
    fn default() -> Self {
        Self {
            lease_ms: 15_000,
            replication_factor: 2,
            split_rows: 25_000_000,
            merge_rows: 2_000_000,
            compact_segments: 8,
            compact_tombstone_ppm: 100_000,
            history_limit: 32,
        }
    }
}

#[derive(Clone)]
pub struct DurableControlPlane {
    state: Arc<Mutex<StoredState>>,
    path: Option<PathBuf>,
    policy: ControlPolicy,
    // Shared by every clone. Set while holding `state`; checked only after
    // acquiring that same lock, so a queued operation cannot miss the failure.
    persistence_uncertain: Arc<AtomicBool>,
    writer: StateWriter,
    // Every clone retains the same open lock file through its lifetime.
    _ownership_lock: Option<Arc<std::fs::File>>,
}

impl DurableControlPlane {
    pub fn open(path: impl Into<PathBuf>, policy: ControlPolicy) -> Result<Self, String> {
        Self::open_state(path.into(), policy, true)
    }

    /// Recover an existing control store without bootstrapping missing history.
    /// A successful reopen reads and syncs the stored decision again. All prior
    /// instances and clones must be dropped to release exclusive file ownership.
    pub fn open_existing(path: impl Into<PathBuf>, policy: ControlPolicy) -> Result<Self, String> {
        Self::open_state(path.into(), policy, false)
    }

    fn open_state(
        path: PathBuf,
        policy: ControlPolicy,
        allow_create: bool,
    ) -> Result<Self, String> {
        let (path, ownership_lock) = storage::acquire(&path, allow_create)?;
        let state = match storage::read(&path) {
            Ok(bytes) => {
                let state: StoredState = serde_json::from_slice(&bytes)
                    .map_err(|error| format!("parse control state {}: {error}", path.display()))?;
                if state.format != 1 {
                    return Err(format!(
                        "control state {} has format {}, expected 1",
                        path.display(),
                        state.format
                    ));
                }
                state
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && allow_create => {
                StoredState::default()
            }
            Err(error) => {
                return Err(format!(
                    "read existing control state {}: {error}",
                    path.display()
                ))
            }
        };
        let this = Self {
            state: Arc::new(Mutex::new(state)),
            path: Some(path),
            policy,
            persistence_uncertain: Arc::new(AtomicBool::new(false)),
            writer: StateWriter::default(),
            _ownership_lock: Some(ownership_lock),
        };
        this.persist()?;
        Ok(this)
    }

    pub fn in_memory(policy: ControlPolicy) -> Self {
        Self {
            state: Arc::new(Mutex::new(StoredState::default())),
            path: None,
            policy,
            persistence_uncertain: Arc::new(AtomicBool::new(false)),
            writer: StateWriter::default(),
            _ownership_lock: None,
        }
    }

    /// Bind this plane to a collection. State written before
    /// collections is adopted and written; state that
    /// names another collection is refused, because its shards are that
    /// dataset's.
    pub fn with_collection(self, name: &str) -> Result<Self, String> {
        {
            let mut state = self
                .lock_state()
                .map_err(|error| error.message().to_string())?;
            let mut candidate = state.clone();
            if candidate.collection.is_empty() {
                candidate.collection = name.to_string();
            } else if candidate.collection != name {
                return Err(format!(
                    "control state {} governs collection {:?}, not {name:?}",
                    self.path
                        .as_ref()
                        .map_or_else(|| "(in memory)".to_string(), |p| p.display().to_string()),
                    candidate.collection
                ));
            }
            self.persist_locked(&candidate)?;
            *state = candidate;
        }
        Ok(self)
    }

    /// Configuration label only; empty for an unnamed dataset. Authority
    /// operations separately check the persistence failure latch under the lock.
    pub fn collection(&self) -> String {
        self.state
            .lock()
            .expect("control state lock poisoned")
            .collection
            .clone()
    }

    /// Seed a pristine control store from the topology already loaded by the
    /// coordinator. Durable state is authoritative after this one-time
    /// bootstrap; a non-pristine store is never overwritten here.
    pub fn bootstrap_topology(
        &self,
        generation: u64,
        routes: &[TopologyRoute],
    ) -> Result<(), String> {
        let routes = routes
            .iter()
            .enumerate()
            .map(|(index, route)| {
                let (hash_lo, hash_hi) = route.hash_range.ok_or_else(|| {
                    format!("control topology route {index} is missing its hash range")
                })?;
                Ok(StoredRoute {
                    addr: route.addr.clone(),
                    replica: route.replica.clone(),
                    hash_lo,
                    hash_hi,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut state = self
            .lock_state()
            .map_err(|error| error.message().to_string())?;
        let pristine = state.topology.generation == 0
            && state.topology.routes.is_empty()
            && state.history.is_empty()
            && state.nodes.is_empty()
            && state.replicas.is_empty()
            && state.actions.is_empty()
            && state.completed_actions.is_empty();
        if !pristine {
            return Ok(());
        }
        let before = state.clone();
        state.topology = StoredTopology { generation, routes };
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(error);
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, StoredState>, Status> {
        let state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        if self.persistence_uncertain.load(Ordering::Acquire) {
            return Err(Status::failed_precondition(UNCERTAIN_CONTROL_STATE));
        }
        Ok(state)
    }

    // Every caller retains the state lock through this write and the failure
    // latch update. A failure before publication leaves the prior authority
    // usable; an ambiguous publication permanently closes this instance.
    fn persist_locked(&self, state: &StoredState) -> Result<(), String> {
        if self.persistence_uncertain.load(Ordering::Acquire) {
            return Err(UNCERTAIN_CONTROL_STATE.to_string());
        }
        match &self.path {
            Some(path) => self.writer.write(path, state).map_err(|error| {
                if error.may_have_published {
                    self.persistence_uncertain.store(true, Ordering::Release);
                    format!("{UNCERTAIN_CONTROL_STATE}: {}", error.message)
                } else {
                    error.message
                }
            }),
            None => Ok(()),
        }
    }

    fn persist(&self) -> Result<(), String> {
        let state = self
            .lock_state()
            .map_err(|error| error.message().to_string())?;
        self.persist_locked(&state)
    }

    fn lease_duration(&self, requested: u64) -> Result<u64, Status> {
        let duration = if requested == 0 {
            self.policy.lease_ms
        } else {
            requested
        };
        if !(1_000..=300_000).contains(&duration) {
            return Err(Status::invalid_argument(
                "lease_ms must be between 1000 and 300000",
            ));
        }
        Ok(duration)
    }
}

/// Whether a call carries cluster membership: a client certificate the
/// listener verified against the cluster CA. One function for the
/// control and operator services; each names its own refusal.
pub(crate) fn cluster_membership<T>(request: &Request<T>, required: bool) -> bool {
    if !required {
        return true;
    }
    #[cfg(feature = "tls")]
    if request.peer_certs().is_some_and(|certs| !certs.is_empty()) {
        return true;
    }
    #[cfg(not(feature = "tls"))]
    let _ = request;
    false
}
