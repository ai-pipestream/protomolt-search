//! Opening and persisting the durable state, leases, registration,
//! reports, and the checks an action's outputs must pass.

use super::*;

impl DurableControlPlane {
    #[cfg(test)]
    pub(crate) fn arm_state_write_fault(&self, fault: StateWriteFault) {
        *self.write_fault.lock().unwrap() = Some(fault);
    }

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
                if bytes.starts_with(RETIRED_CONTROL_MAGIC) {
                    return Err(RETIRED_CONTROL_STATE.to_string());
                }
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
            #[cfg(test)]
            write_fault: Arc::new(Mutex::new(None)),
            _ownership_lock: Some(ownership_lock),
            #[cfg(feature = "net")]
            retirement: Arc::new(std::sync::OnceLock::new()),
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
            #[cfg(test)]
            write_fault: Arc::new(Mutex::new(None)),
            _ownership_lock: None,
            #[cfg(feature = "net")]
            retirement: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Bind this plane to a collection (`docs/collections.md`). State
    /// written before collections is adopted and written; state that
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

    pub(super) fn lock_state(&self) -> Result<MutexGuard<'_, StoredState>, Status> {
        let state = self
            .state
            .lock()
            .map_err(|_| Status::internal("control state lock poisoned"))?;
        if self.persistence_uncertain.load(Ordering::Acquire) {
            return Err(Status::failed_precondition(UNCERTAIN_CONTROL_STATE));
        }
        #[cfg(feature = "net")]
        if self.retirement.get().is_some() {
            return Err(Status::failed_precondition(RETIRED_CONTROL_STATE));
        }
        Ok(state)
    }

    // Every caller retains the state lock through this write and the failure
    // latch update. A failure before publication leaves the prior authority
    // usable; an ambiguous publication permanently closes this instance.
    pub(super) fn persist_locked(&self, state: &StoredState) -> Result<(), String> {
        if self.persistence_uncertain.load(Ordering::Acquire) {
            return Err(UNCERTAIN_CONTROL_STATE.to_string());
        }
        #[cfg(feature = "net")]
        if self.retirement.get().is_some() {
            return Err(RETIRED_CONTROL_STATE.to_string());
        }
        match &self.path {
            Some(path) => write_state(
                path,
                state,
                #[cfg(test)]
                &self.write_fault,
            )
            .map_err(|error| {
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

    pub(super) fn validate_lease<'a>(
        state: &'a mut StoredState,
        node_id: &str,
        token: u64,
        now: u64,
    ) -> Result<&'a mut StoredNode, Status> {
        let node = state
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| Status::not_found(format!("node {node_id:?} is not registered")))?;
        if token == 0 || token != node.lease_token {
            return Err(Status::failed_precondition(format!(
                "node {node_id:?} lease token does not match"
            )));
        }
        if node.expires_unix_ms <= now {
            return Err(Status::failed_precondition(format!(
                "node {node_id:?} lease expired at {}",
                node.expires_unix_ms
            )));
        }
        Ok(node)
    }

    pub(super) fn register(
        &self,
        request: RegisterNodeRequest,
        now: u64,
    ) -> Result<NodeLease, Status> {
        if request.node_id.trim().is_empty() || request.addr.trim().is_empty() {
            return Err(Status::invalid_argument(
                "node registration needs non-empty node_id and addr",
            ));
        }
        let duration = self.lease_duration(request.lease_ms)?;
        let mut state = self.lock_state()?;
        if let Some(held) = state.nodes.get(&request.node_id) {
            if held.state != StoredNodeState::Expired && held.addr != request.addr {
                return Err(Status::already_exists(format!(
                    "node {:?} is already leased at {:?}",
                    request.node_id, held.addr
                )));
            }
        }
        let token = state.next_token;
        let next_token = token
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("lease token space exhausted"))?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let expires = now
            .checked_add(duration)
            .ok_or_else(|| Status::resource_exhausted("lease expiry overflow"))?;
        let before = state.clone();
        state.next_token = next_token;
        state.nodes.insert(
            request.node_id.clone(),
            StoredNode {
                node_id: request.node_id.clone(),
                addr: request.addr,
                state: StoredNodeState::Active,
                lease_token: token,
                expires_unix_ms: expires,
                capacity: request.capacity.unwrap_or_default().into(),
            },
        );
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(NodeLease {
            node_id: request.node_id,
            lease_token: token,
            expires_unix_ms: expires,
            control_revision: state.revision,
        })
    }

    pub(super) fn renew(
        &self,
        request: RenewNodeLeaseRequest,
        now: u64,
    ) -> Result<NodeLease, Status> {
        let duration = self.lease_duration(request.lease_ms)?;
        let mut state = self.lock_state()?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let node = Self::validate_lease(&mut state, &request.node_id, request.lease_token, now)?;
        node.expires_unix_ms = now
            .checked_add(duration)
            .ok_or_else(|| Status::resource_exhausted("lease expiry overflow"))?;
        if let Some(capacity) = request.capacity {
            node.capacity = capacity.into();
        }
        let lease = NodeLease {
            node_id: node.node_id.clone(),
            lease_token: node.lease_token,
            expires_unix_ms: node.expires_unix_ms,
            control_revision: revision,
        };
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(lease)
    }

    pub(super) fn drain(&self, request: DrainNodeRequest, now: u64) -> Result<(), Status> {
        let mut state = self.lock_state()?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let node = Self::validate_lease(&mut state, &request.node_id, request.lease_token, now)?;
        node.state = StoredNodeState::Draining;
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(())
    }

    pub(super) fn report(&self, request: ReportShardRequest, now: u64) -> Result<(), Status> {
        let replica = request
            .replica
            .ok_or_else(|| Status::invalid_argument("shard report is missing replica state"))?;
        if replica.shard_id.trim().is_empty() {
            return Err(Status::invalid_argument("shard report needs shard_id"));
        }
        let role = match ShardReplicaRole::try_from(replica.role) {
            Ok(ShardReplicaRole::Primary) => StoredRole::Primary,
            Ok(ShardReplicaRole::Replica) => StoredRole::Replica,
            _ => {
                return Err(Status::invalid_argument(
                    "shard report needs PRIMARY or REPLICA role",
                ))
            }
        };
        if replica.hash_lo > replica.hash_hi {
            return Err(Status::invalid_argument(
                "shard report hash range is inverted",
            ));
        }
        if replica.tombstones > replica.rows {
            return Err(Status::invalid_argument(
                "shard tombstone count exceeds physical rows",
            ));
        }
        let mut state = self.lock_state()?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let node_addr =
            Self::validate_lease(&mut state, &request.node_id, request.lease_token, now)?
                .addr
                .clone();
        if !replica.node_id.is_empty() && replica.node_id != request.node_id {
            return Err(Status::invalid_argument(
                "shard report node_id differs from the lease owner",
            ));
        }
        let addr = Self::shard_addr(&state, &request.node_id, &node_addr, replica.addr)?;
        let key = replica_key(&replica.shard_id, &request.node_id);
        state.replicas.insert(
            key,
            StoredReplica {
                shard_id: replica.shard_id,
                node_id: request.node_id,
                addr,
                generation: replica.generation,
                hash_lo: replica.hash_lo,
                hash_hi: replica.hash_hi,
                slot_offset: replica.slot_offset,
                rows: replica.rows,
                bytes: replica.bytes,
                role,
                ready: replica.ready,
                scoring_fingerprint: replica.scoring_fingerprint,
                analysis_fingerprint: replica.analysis_fingerprint,
                immutable_segments: replica.immutable_segments,
                tombstones: replica.tombstones,
            },
        );
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(())
    }

    /// The listener a reported copy is served on: the node's registered
    /// address when the report leaves it empty, else the address the
    /// report names — the lease owner vouches for its own listeners
    /// (one per shard, one per placed replica), and another node's
    /// registered address is not one of them.
    fn shard_addr(
        state: &StoredState,
        node_id: &str,
        node_addr: &str,
        reported: String,
    ) -> Result<String, Status> {
        if reported.is_empty() {
            return Ok(node_addr.to_string());
        }
        if let Some(other) = state
            .nodes
            .values()
            .find(|other| other.node_id != node_id && other.addr == reported)
        {
            return Err(Status::invalid_argument(format!(
                "shard addr {reported:?} is the registered address of node {:?}, not a listener \
                 of node {node_id:?}",
                other.node_id
            )));
        }
        Ok(reported)
    }

    pub(super) fn checked_output(
        state: &StoredState,
        target_node_id: &str,
        output: ShardReplicaState,
    ) -> Result<StoredReplica, Status> {
        if output.shard_id.trim().is_empty() || !output.ready {
            return Err(Status::invalid_argument(
                "action outputs need a non-empty shard_id and ready=true",
            ));
        }
        if output.hash_lo > output.hash_hi || output.tombstones > output.rows {
            return Err(Status::invalid_argument(
                "action output has an invalid range or tombstone count",
            ));
        }
        if !output.node_id.is_empty() && output.node_id != target_node_id {
            return Err(Status::invalid_argument(
                "action output node_id differs from the assigned target",
            ));
        }
        let node_addr = state
            .nodes
            .get(target_node_id)
            .ok_or_else(|| Status::failed_precondition("action target is no longer registered"))?
            .addr
            .clone();
        let addr = Self::shard_addr(state, target_node_id, &node_addr, output.addr)?;
        let role = match ShardReplicaRole::try_from(output.role) {
            Ok(ShardReplicaRole::Primary) => StoredRole::Primary,
            Ok(ShardReplicaRole::Replica) => StoredRole::Replica,
            _ => {
                return Err(Status::invalid_argument(
                    "action output needs PRIMARY or REPLICA role",
                ))
            }
        };
        Ok(StoredReplica {
            shard_id: output.shard_id,
            node_id: target_node_id.to_string(),
            addr,
            generation: output.generation,
            hash_lo: output.hash_lo,
            hash_hi: output.hash_hi,
            slot_offset: output.slot_offset,
            rows: output.rows,
            bytes: output.bytes,
            role,
            ready: true,
            scoring_fingerprint: output.scoring_fingerprint,
            analysis_fingerprint: output.analysis_fingerprint,
            immutable_segments: output.immutable_segments,
            tombstones: output.tombstones,
        })
    }

    pub(super) fn require_same_contract(
        output: &StoredReplica,
        source: &StoredReplica,
        generation: u64,
    ) -> Result<(), Status> {
        if output.generation != generation
            || output.scoring_fingerprint != source.scoring_fingerprint
            || output.analysis_fingerprint != source.analysis_fingerprint
        {
            return Err(Status::failed_precondition(
                "action output generation or scoring/analysis fingerprint differs from the plan",
            ));
        }
        Ok(())
    }

    pub(super) fn require_range_tiling(
        outputs: &[StoredReplica],
        hash_lo: u64,
        hash_hi: u64,
    ) -> Result<(), Status> {
        let mut ranges = outputs
            .iter()
            .map(|output| (output.hash_lo, output.hash_hi))
            .collect::<Vec<_>>();
        ranges.sort_unstable();
        let mut expected = hash_lo;
        for (lo, hi) in ranges {
            if lo != expected || hi < lo {
                return Err(Status::failed_precondition(
                    "action outputs do not exactly tile the source hash range",
                ));
            }
            expected = hi.checked_add(1).unwrap_or(0);
        }
        if outputs.is_empty()
            || outputs.iter().map(|output| output.hash_hi).max() != Some(hash_hi)
            || (hash_hi != u64::MAX && expected != hash_hi + 1)
        {
            return Err(Status::failed_precondition(
                "action outputs do not exactly tile the source hash range",
            ));
        }
        Ok(())
    }
}
