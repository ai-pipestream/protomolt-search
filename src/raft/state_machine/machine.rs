//! The state machine over the store's committed-replay paths.

use super::*;

impl ControlStateMachine {
    /// Wrap an opened store. The store is marked Raft-hosted: its direct
    /// command paths refuse from here on. Abandoned receives and builds and
    /// unpublished generations are removed; the published one is kept.
    pub(crate) fn new(
        store: SourceAuthorityStore,
        snapshots: &Path,
        max_image_bytes: u64,
    ) -> Result<Self, Status> {
        store.set_raft_hosted();
        std::fs::create_dir_all(snapshots.join(GENERATIONS)).map_err(io)?;
        let membership = match store.raft_applied()? {
            Some(applied) => stored_membership_from_proto(
                applied
                    .membership
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("raft applied position has no membership"))?,
            )?,
            None => StoredMembership::new(None, Membership::new(Vec::new(), BTreeMap::new())),
        };
        let store_identity = store.identity().clone();
        #[cfg(any(test, feature = "fault-injection"))]
        let gates = Arc::new(SnapshotGates::default());
        let machine = Self {
            identity: store.identity().clone(),
            store: Arc::new(RwLock::new(Some(store))),
            snapshots: snapshots.to_path_buf(),
            membership: Mutex::new(membership),
            staging: Arc::new(SnapshotStaging {
                snapshots: snapshots.to_path_buf(),
                identity: store_identity,
                max_image_bytes,
                receiving: Mutex::new(None),
                counter: AtomicU64::new(0),
                owned: Arc::new(tokio::sync::Semaphore::new(MAX_OWNED_RECEIVES)),
                #[cfg(any(test, feature = "fault-injection"))]
                gates: Arc::clone(&gates),
            }),
            last_build: Arc::new(Mutex::new(None)),
            snapshot_lock: Arc::new(Mutex::new(())),
            pointer_lock: Arc::new(Mutex::new(())),
            #[cfg(any(test, feature = "fault-injection"))]
            gates,
            #[cfg(any(test, feature = "fault-injection"))]
            install_fault: Arc::new(Mutex::new(None)),
        };
        machine.sweep()?;
        Ok(machine)
    }

    /// The staging area the transport receives and validates images in.
    pub(crate) fn staging(&self) -> Arc<SnapshotStaging> {
        Arc::clone(&self.staging)
    }

    /// The latest build's report, shared with the host.
    pub(crate) fn last_build(&self) -> LastBuild {
        Arc::clone(&self.last_build)
    }

    /// The published generation, for a check of its image by digest from
    /// outside the machine (the host, on a peer's digest rejection).
    pub(crate) fn published_image(&self) -> Arc<PublishedImage> {
        Arc::new(PublishedImage {
            identity: self.identity.clone(),
            snapshots: self.snapshots.clone(),
            pointer_lock: Arc::clone(&self.pointer_lock),
        })
    }

    /// The pause hooks on the snapshot build, serve and install paths.
    #[cfg(any(test, feature = "fault-injection"))]
    pub(crate) fn gates(&self) -> Arc<SnapshotGates> {
        Arc::clone(&self.gates)
    }

    /// The exit fault slot on the install path, shared with the host.
    #[cfg(any(test, feature = "fault-injection"))]
    pub(crate) fn install_fault(&self) -> InstallFault {
        Arc::clone(&self.install_fault)
    }

    #[cfg(any(test, feature = "fault-injection"))]
    pub(super) fn inject_install(&self, after: bool) {
        use crate::source_authority::ExitFault;
        let fault = self.install_fault.lock().unwrap();
        match (*fault, after) {
            (Some(ExitFault::BeforeCommit), false) | (Some(ExitFault::AfterCommit), true) => {
                std::process::exit(87)
            }
            _ => {}
        }
    }

    pub(crate) fn shared_store(&self) -> SharedStore {
        Arc::clone(&self.store)
    }

    pub(super) fn store(&self) -> Result<SourceAuthorityStore, Status> {
        self.store
            .read()
            .map_err(|_| Status::internal("state machine store lock poisoned"))?
            .clone()
            .ok_or_else(|| Status::unavailable("state machine store is being replaced"))
    }

    /// Remove everything but the published generation: partial builds,
    /// abandoned receives and generations whose pointer never landed.
    pub(super) fn sweep(&self) -> Result<(), Status> {
        let published = read_pointer(&self.snapshots)?;
        for entry in std::fs::read_dir(&self.snapshots).map_err(io)? {
            let entry = entry.map_err(io)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("incoming-") || name.starts_with("build-") || name == STAGED {
                if entry.path().is_dir() {
                    std::fs::remove_dir_all(entry.path()).map_err(io)?;
                } else {
                    std::fs::remove_file(entry.path()).map_err(io)?;
                }
            }
        }
        for entry in std::fs::read_dir(self.snapshots.join(GENERATIONS)).map_err(io)? {
            let entry = entry.map_err(io)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.parse::<u64>().ok() != published {
                std::fs::remove_dir_all(entry.path()).map_err(io)?;
            }
        }
        if let Some(generation) = published {
            let dir = self
                .snapshots
                .join(GENERATIONS)
                .join(generation.to_string());
            // A pointer without a complete generation is data loss, never a
            // reason to start over.
            let (meta, _) = read_generation(&dir, &self.identity)?;
            // A generation is an image of this store at its applied position;
            // one claiming a position the store never applied is data loss
            // too (a forged pointer, or a store restored from an older copy),
            // and the library's own invariants would fail on it.
            let claimed = meta.last_log_id.map(|id| id.index);
            let applied = self
                .store()?
                .raft_applied()?
                .and_then(|a| a.last_applied)
                .map(|id| id.index);
            if claimed > applied {
                return Err(Status::data_loss(format!(
                    "published snapshot generation {generation} claims position {} past the store's applied position {}",
                    claimed.map_or_else(|| "none".to_string(), |i| i.to_string()),
                    applied.map_or_else(|| "none".to_string(), |i| i.to_string())
                )));
            }
        }
        Ok(())
    }

    pub(super) fn dispatch(
        store: &SourceAuthorityStore,
        proposal: &RaftProposal,
    ) -> Result<Reply, Status> {
        let principal = proposal.principal.as_str();
        match proposal
            .command
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("raft proposal carries no command"))?
        {
            Command::Control(command) => {
                Ok(Reply::Control(store.replay_command(principal, command)?))
            }
            Command::Install(command) => Ok(Reply::Install(
                store.replay_control_install(principal, command)?,
            )),
            Command::Capacity(command) => Ok(Reply::Capacity(
                store.replay_capacity_configure(principal, command)?,
            )),
            Command::Observation(transition) => Ok(Reply::Observation(
                store.replay_capacity_transition(principal, transition)?,
            )),
        }
    }

    /// The applied position the store recorded must be the entry just
    /// applied; anything else means an application path skipped it.
    pub(super) fn confirm_position(
        store: &SourceAuthorityStore,
        entry: &LogId<NodeId>,
    ) -> Result<(), Status> {
        let applied = store
            .raft_applied()?
            .and_then(|a| a.last_applied)
            .map(|id| log_id_from_proto(&id));
        if applied.as_ref() != Some(entry) {
            return Err(Status::internal(format!(
                "applied position {applied:?} differs from the entry {entry} just applied"
            )));
        }
        Ok(())
    }
}

impl ControlStateMachine {
    pub(super) fn install_received(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        receive: &Receive,
    ) -> Result<(), Status> {
        #[cfg(any(test, feature = "fault-injection"))]
        self.gates.pause_install();
        let _serial = self.snapshot_lock.lock().unwrap();
        // The transport validates the complete image before the library's
        // install runs; a receive that arrived another way is validated
        // here, on the same rules.
        let signature = if receive.verified.as_deref() == Some(meta.snapshot_id.as_str()) {
            parse_snapshot_id(&meta.snapshot_id)?
        } else {
            self.staging.verify(&receive.dir, meta)?
        };
        write_meta(&receive.dir, &self.identity, meta, &signature)?;
        let generation = next_generation(&self.snapshots)?;
        let target = self
            .snapshots
            .join(GENERATIONS)
            .join(generation.to_string());
        std::fs::rename(&receive.dir, &target).map_err(io)?;
        sync_dir(&self.snapshots.join(GENERATIONS))?;
        // Swap the live store to a fresh copy of the verified image in
        // place; the generation's image stays pristine. Every handle sees
        // the installed state on its next operation; on refusal nothing
        // changed and the unpublished generation is removed.
        let staged = self.snapshots.join(STAGED);
        std::fs::copy(target.join(IMAGE), &staged).map_err(io)?;
        std::fs::File::open(&staged)
            .and_then(|f| f.sync_all())
            .map_err(io)?;
        let store = self.store()?;
        // The install's commit is the store's replacement: from here the
        // store is at the image's position. The generation's publication
        // is a second step; a node that stops between the two starts with
        // the store at the position and no generation published.
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject_install(false);
        if let Err(status) = store.replace_from(&staged) {
            let _ = std::fs::remove_file(&staged);
            let _ = std::fs::remove_dir_all(&target);
            return Err(status);
        }
        #[cfg(any(test, feature = "fault-injection"))]
        self.inject_install(true);
        drop(store);
        publish(&self.pointer_lock, &self.snapshots, generation)?;
        *self.membership.lock().unwrap() = meta.last_membership.clone();
        Ok(())
    }
}
