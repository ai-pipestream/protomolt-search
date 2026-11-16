//! Creating, opening and replacing the store's database file.
use super::*;

impl SourceAuthorityStore {
    /// Explicit trusted bootstrap in an existing durable directory. The supplied
    /// identity, limits and resource bindings become immutable. No source exists
    /// as a consequence of creating this control database.
    pub fn create(
        path: &Path,
        identity: &SourceAuthorityIdentity,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
    ) -> Result<Self, Status> {
        contract::identity(identity)?;
        contract::limits(limits)?;
        contract::policy(policy)?;
        if policy.encoded_len() as u64 > limits.max_payload_bytes {
            return Err(Status::resource_exhausted(
                "source authority bootstrap policy exceeds payload capacity",
            ));
        }
        let (store, parent) = Self::open_file(path, identity, true)?;
        let header = SourceAuthorityHeader {
            format_version: 1,
            identity: Some(identity.clone()),
            limits: Some(*limits),
            control_revision: 1,
            payload_bytes: policy.encoded_len() as u64,
            ..Default::default()
        };
        let mut tx = store.inner.database().begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            meta.insert("header", header.encode_to_vec().as_slice())
                .map_err(storage)?;
            meta.insert("policy", policy.encode_to_vec().as_slice())
                .map_err(storage)?;
            tx.open_table(OWNERS).map_err(storage)?;
            tx.open_table(DECISIONS).map_err(storage)?;
            tx.open_table(WORKFLOWS).map_err(storage)?;
        }
        control::create_tables(&tx)?;
        capacity::create_tables(&tx)?;
        tx.commit().map_err(storage)?;
        parent.sync_all().map_err(storage)?;
        store.inner.revisions.send_replace(policy.revision);
        store.inner.applied.send_replace(header.control_revision);
        Ok(store)
    }

    /// The store of a member that will be seeded by the group's snapshot:
    /// a bootstrap image marked pending, on which no log entry applies
    /// until the leader's image replaces it.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    pub(crate) fn create_member(
        path: &Path,
        identity: &SourceAuthorityIdentity,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
    ) -> Result<Self, Status> {
        let store = Self::create(path, identity, policy, limits)?;
        let mut tx = store.inner.database().begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            meta.insert(MEMBER_META, MEMBER_PENDING).map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        Ok(store)
    }

    /// Whether this store is a prepared member still waiting for the
    /// group's first snapshot.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    pub(crate) fn awaiting_snapshot(&self) -> Result<bool, Status> {
        self.guarded(|| {
            let tx = self.inner.database().begin_read().map_err(storage)?;
            let meta = tx.open_table(META).map_err(storage)?;
            Ok(meta.get(MEMBER_META).map_err(storage)?.is_some())
        })
    }

    /// Existing-state recovery only. An empty/missing/corrupt authority never
    /// initializes a fresh identity, policy or retry history.
    pub fn open(path: &Path, expected_identity: &SourceAuthorityIdentity) -> Result<Self, Status> {
        contract::identity(expected_identity)?;
        let (store, _) = Self::open_file(path, expected_identity, false)?;
        {
            let backing = store.inner.database();
            recovery::validate(&backing, expected_identity)?;
        }
        let (revision, applied) = {
            let tx = store.inner.database().begin_read().map_err(storage)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let policy: AccessPolicy = contract::decode(
                meta.get("policy")
                    .map_err(storage)?
                    .ok_or_else(|| missing("policy"))?
                    .value(),
            )?;
            let header: SourceAuthorityHeader = contract::decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| missing("header"))?
                    .value(),
            )?;
            (policy.revision, header.control_revision)
        };
        store.inner.revisions.send_replace(revision);
        store.inner.applied.send_replace(applied);
        Ok(store)
    }

    /// Open (or create) the file at `path` under its exclusive lock, with
    /// its database. Nothing here reads the store's content.
    fn open_backing(path: &Path, create: bool) -> Result<(Backing, File), Status> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let directory = File::open(parent).map_err(storage)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(create);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path).map_err(|error| match error.kind() {
            std::io::ErrorKind::AlreadyExists => {
                Status::already_exists("source authority already exists")
            }
            std::io::ErrorKind::NotFound => Status::not_found(
                "source authority is missing; recovery never bootstraps a new history",
            ),
            _ => storage(error),
        })?;
        #[cfg(any(test, feature = "fork-guard"))]
        let _handoff = crate::test_support::lock_handoff();
        file.try_lock().map_err(|error| {
            Status::failed_precondition(format!("source authority exclusive file lock: {error}"))
        })?;
        if !create && file.metadata().map_err(storage)?.len() == 0 {
            return Err(corrupt("existing file is empty"));
        }
        let mut builder = Database::builder();
        builder.set_cache_size(8 * 1024 * 1024);
        let backend = redb::backends::FileBackend::new(file.try_clone().map_err(storage)?)
            .map_err(storage)?;
        let database = builder.create_with_backend(backend).map_err(storage)?;
        Ok((
            Backing {
                database,
                _file_lock: file,
            },
            directory,
        ))
    }

    fn open_file(
        path: &Path,
        identity: &SourceAuthorityIdentity,
        create: bool,
    ) -> Result<(Self, File), Status> {
        let (backing, directory) = Self::open_backing(path, create)?;
        Ok((
            Self {
                inner: Arc::new(Inner {
                    backing: RwLock::new(backing),
                    admission: RwLock::new(()),
                    revisions: watch::Sender::new(0),
                    applied: watch::Sender::new(0),
                    closed: Mutex::new(false),
                    identity: identity.clone(),
                    path: path.to_path_buf(),
                    hosted: AtomicBool::new(false),
                    raft_pending: Mutex::new(None),
                    #[cfg(any(test, feature = "fault-injection"))]
                    fault: Mutex::new(None),
                }),
            },
            directory,
        ))
    }

    /// The committed policy revision and control revision of a database.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    fn revisions_of(database: &Database) -> Result<(u64, u64), Status> {
        let tx = database.begin_read().map_err(storage)?;
        let meta = tx.open_table(META).map_err(storage)?;
        let policy: AccessPolicy = contract::decode(
            meta.get("policy")
                .map_err(storage)?
                .ok_or_else(|| missing("policy"))?
                .value(),
        )?;
        let header: SourceAuthorityHeader = contract::decode(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| missing("header"))?
                .value(),
        )?;
        Ok((policy.revision, header.control_revision))
    }

    /// Replace this store's database in place with a verified staged image.
    /// The staged file is opened, adopted and validated as this identity
    /// first; only then is it renamed over the store's path and swapped in.
    /// The swap holds the admission lock exclusively and the operation
    /// lock, so no admitted work and no transaction spans it; every handle,
    /// held or not, observes the installed state on its next operation, and
    /// the policy and applied watches publish the installed revisions. On
    /// any refusal nothing changed and the handle serves as before.
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    pub(crate) fn replace_from(&self, staged: &Path) -> Result<(), Status> {
        let _exclusive = self.exclusive()?;
        let mut closed = self
            .inner
            .closed
            .lock()
            .map_err(|_| storage("authority lock poisoned"))?;
        if *closed {
            return Err(Status::failed_precondition(CLOSED));
        }
        if self.inner.raft_pending.lock().unwrap().is_some() {
            return Err(Status::internal(
                "a committed apply is in flight; the store cannot be replaced under it",
            ));
        }
        let (backing, _directory) = Self::open_backing(staged, false)?;
        recovery::validate(&backing, &self.inner.identity)?;
        let (revision, control_revision) = Self::revisions_of(&backing)?;
        std::fs::rename(staged, &self.inner.path).map_err(storage)?;
        let parent = self
            .inner
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)
            .and_then(|d| d.sync_all())
            .map_err(storage)?;
        {
            let mut slot = self
                .inner
                .backing
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // The old database and its file lock drop here, after the new
            // file is durably in place and its own lock is held.
            *slot = backing;
        }
        *closed = false;
        self.inner.revisions.send_replace(revision);
        self.inner.applied.send_replace(control_revision);
        Ok(())
    }
}
