//! Opening, creating and initializing a catalog file, and the in-place
//! upgrade of a legacy history format.
use super::*;

impl DocumentCatalog {
    /// Reopen an existing authority. A missing file must never initialize new
    /// version or retry history; callers explicitly create a new catalog.
    pub fn open(path: &Path, collection: &str) -> Result<Self, Status> {
        Self::open_file(path, collection, false, None)
    }

    pub fn create(path: &Path, collection: &str) -> Result<Self, Status> {
        Self::open_file(path, collection, true, None)
    }

    pub(super) fn open_file(
        path: &Path,
        collection: &str,
        create: bool,
        resource_binding: Option<SourceResourceBinding>,
    ) -> Result<Self, Status> {
        Self::open_bound_file(path, collection, create, resource_binding, None)
    }

    pub(super) fn open_bound_file(
        path: &Path,
        collection: &str,
        create: bool,
        resource_binding: Option<SourceResourceBinding>,
        managed_binding: Option<SourceManagedBinding>,
    ) -> Result<Self, Status> {
        if create && managed_binding.is_some() {
            return Err(Status::failed_precondition(
                "managed source recovery never creates or replaces source history",
            ));
        }
        Self::open_file_resolving_binding(path, collection, create, resource_binding, |_| {
            Ok((managed_binding, None))
        })
    }

    // Resolve expected managed metadata only after taking the existing file's
    // exclusive lock, retained through validation and for the returned handle.
    pub(super) fn open_file_resolving_binding(
        path: &Path,
        collection: &str,
        create: bool,
        resource_binding: Option<SourceResourceBinding>,
        resolve_binding: impl FnOnce(
            &Database,
        ) -> Result<
            (
                Option<SourceManagedBinding>,
                Option<SourceManagedActivation>,
            ),
            Status,
        >,
    ) -> Result<Self, Status> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        // The application supplies an existing durable container directory.
        // Creating ancestors here would require syncing each ancestor as well.
        let opening_error = |error: std::io::Error| match error.kind() {
            std::io::ErrorKind::AlreadyExists => {
                Status::already_exists("document catalog already exists")
            }
            std::io::ErrorKind::NotFound if !create => {
                Status::not_found("document catalog is missing; open never creates a new authority")
            }
            _ => storage(error),
        };
        let directory = File::open(parent).map_err(opening_error)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(create);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path).map_err(opening_error)?;
        let new = create;
        #[cfg(any(test, feature = "fork-guard"))]
        let _handoff = crate::test_support::lock_handoff();
        file.try_lock().map_err(|e| {
            Status::failed_precondition(format!("exclusive document catalog lock unavailable: {e}"))
        })?;
        if !new && file.metadata().map_err(storage)?.len() == 0 {
            return Err(Status::data_loss("existing document catalog is empty"));
        }
        let mut builder = Database::builder();
        builder.set_cache_size(CACHE_BYTES);
        let backend = redb::backends::FileBackend::new(file.try_clone().map_err(storage)?)
            .map_err(storage)?;
        let database = builder.create_with_backend(backend).map_err(storage)?;
        let (managed_binding, managed_activation) = resolve_binding(&database)?;
        let catalog = Self {
            database,
            durable: true,
            resource_binding,
            managed_binding,
            managed_activation,
            _file_lock: Some(file),
            #[cfg(any(test, feature = "fault-injection"))]
            precommit_pause: std::sync::Mutex::new(None),
            #[cfg(any(test, feature = "fault-injection"))]
            precommit_gate: std::sync::Mutex::new(None),
            #[cfg(any(test, feature = "fault-injection"))]
            postcommit_pause: std::sync::Mutex::new(None),
        };
        catalog.initialize(collection, new)?;
        catalog.validate_projection_journal()?;
        directory.sync_all().map_err(storage)?;
        Ok(catalog)
    }

    pub fn in_memory(collection: &str) -> Result<Self, Status> {
        let mut builder = Database::builder();
        builder.set_cache_size(CACHE_BYTES);
        let database = builder
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(storage)?;
        let catalog = Self {
            database,
            durable: false,
            resource_binding: None,
            managed_binding: None,
            managed_activation: None,
            _file_lock: None,
            #[cfg(any(test, feature = "fault-injection"))]
            precommit_pause: std::sync::Mutex::new(None),
            #[cfg(any(test, feature = "fault-injection"))]
            precommit_gate: std::sync::Mutex::new(None),
            #[cfg(any(test, feature = "fault-injection"))]
            postcommit_pause: std::sync::Mutex::new(None),
        };
        catalog.initialize(collection, true)?;
        Ok(catalog)
    }

    fn initialize(&self, collection: &str, new: bool) -> Result<(), Status> {
        if new {
            self.initialize_new(collection)
        } else {
            self.validate_existing(collection)
        }
    }

    /// Check an existing file's header, bindings and tables, upgrading a
    /// legacy format in place.
    fn validate_existing(&self, collection: &str) -> Result<(), Status> {
        let transaction = self.database.begin_read().map_err(storage)?;
        let table = transaction.open_table(META).map_err(storage)?;
        let bytes = table
            .get("header")
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("existing document catalog header missing"))?;
        let header: DocumentCatalogHeader = decode_header(bytes.value())?;
        if !(1..=ACTIVE_MANAGED_FORMAT).contains(&header.format_version)
            || header.collection != collection
        {
            return Err(Status::failed_precondition(
                "document catalog format or collection differs",
            ));
        }
        if header.format_version >= FORMAT_VERSION {
            validate_current_header(&header)?;
        } else if !header.history_id.is_empty()
            || header.legacy_receipts_through_sequence != 0
            || header.history_seal.is_some()
            || header.retirement_intent.is_some()
            || header.resource_binding.is_some()
            || header.actor_namespace.is_some()
            || header.managed_binding.is_some()
            || header.managed_activation.is_some()
        {
            return Err(Status::data_loss(
                "legacy catalog contains unexpected history identity metadata",
            ));
        }
        self.validate_resource_binding(&header)?;
        for definition in [HEADS, VERSIONS, OPERATIONS, DESCRIPTORS, SOURCES] {
            transaction.open_table(definition).map_err(storage)?;
        }
        if header.format_version < FORMAT_VERSION {
            if header.format_version == 2 {
                transaction.open_table(CHANGES).map_err(storage)?;
            }
            drop(bytes);
            drop(table);
            drop(transaction);
            return self.upgrade_history();
        }
        transaction.open_table(CHANGES).map_err(storage)?;
        actors::validate_read_counts(&transaction, &header)?;
        if header.format_version == LEGACY_ACCESS_CONTROLLED_FORMAT && header.accepted_sequence == 0
        {
            drop(bytes);
            drop(table);
            drop(transaction);
            return self.upgrade_empty_actor_namespace();
        }
        Ok(())
    }

    /// Write a new catalog's header and create its tables.
    fn initialize_new(&self, collection: &str) -> Result<(), Status> {
        let mut transaction = self.database.begin_write().map_err(storage)?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(storage)?;
        {
            let mut table = transaction.open_table(META).map_err(storage)?;
            let header = DocumentCatalogHeader {
                format_version: if self.resource_binding.is_some() {
                    ACCESS_CONTROLLED_FORMAT
                } else {
                    FORMAT_VERSION
                },
                collection: collection.into(),
                accepted_sequence: 0,
                history_id: new_history_id()?,
                legacy_receipts_through_sequence: 0,
                history_seal: None,
                retirement_intent: None,
                resource_binding: self.resource_binding.clone(),
                actor_namespace: self.resource_binding.as_ref().map(|_| actors::namespace(0)),
                managed_binding: None,
                managed_activation: None,
            }
            .encode_to_vec();
            table.insert("header", header.as_slice()).map_err(storage)?;
        }
        for definition in [HEADS, VERSIONS, OPERATIONS, DESCRIPTORS, SOURCES] {
            transaction.open_table(definition).map_err(storage)?;
        }
        transaction.open_table(CHANGES).map_err(storage)?;
        if self.resource_binding.is_some() {
            transaction
                .open_table(actors::OPERATIONS)
                .map_err(storage)?;
        }
        transaction.commit().map_err(storage)
    }

    fn upgrade_history(&self) -> Result<(), Status> {
        let mut transaction = self.database.begin_write().map_err(storage)?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(storage)?;
        {
            let mut meta = transaction.open_table(META).map_err(storage)?;
            let mut header: DocumentCatalogHeader = decode_header(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("catalog header missing"))?
                    .value(),
            )?;
            if header.format_version == 1 {
                let versions = transaction.open_table(VERSIONS).map_err(storage)?;
                let mut changes = transaction.open_table(CHANGES).map_err(storage)?;
                if !changes.is_empty().map_err(storage)? {
                    return Err(Status::data_loss(
                        "version-1 catalog already contains a change index",
                    ));
                }
                for entry in versions.iter().map_err(storage)? {
                    let (key, value) = entry.map_err(storage)?;
                    let version: DocumentVersion = decode(value.value())?;
                    let valid_key =
                        !version.document_key.is_empty() && version.document_key.len() <= 16 * 1024;
                    let expected = DocumentVersionKey {
                        document_key: version.document_key,
                        version: version.version,
                    }
                    .encode_to_vec();
                    if version.version == 0
                        || !valid_key
                        || key.value() != expected
                        || version.accepted_sequence == 0
                        || version.accepted_sequence > header.accepted_sequence
                    {
                        return Err(Status::data_loss("invalid version in catalog history"));
                    }
                    if changes
                        .insert(version.accepted_sequence, key.value())
                        .map_err(storage)?
                        .is_some()
                    {
                        return Err(Status::data_loss("duplicate sequence in catalog history"));
                    }
                }
                if changes.len().map_err(storage)? != header.accepted_sequence {
                    return Err(Status::data_loss(
                        "catalog history has missing accepted versions",
                    ));
                }
            }
            header.history_id = new_history_id()?;
            header.legacy_receipts_through_sequence = header.accepted_sequence;
            header.format_version = FORMAT_VERSION;
            meta.insert("header", header.encode_to_vec().as_slice())
                .map_err(storage)?;
        }
        transaction.commit().map_err(storage)
    }
}
