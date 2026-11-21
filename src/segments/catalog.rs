//! Catalog snapshots, durable evidence and row-appending publication.

use super::*;

impl SegmentCatalog {
    pub(super) fn check_source_owner(
        &self,
        owner: Option<&crate::pb::storage::SourceIndexOwner>,
    ) -> Result<(), String> {
        self.check_disk_identity()?;
        let proposed = owner.map(SegmentSourceOwner::encode).transpose()?;
        let current = self.snapshot();
        let disk = Self::read_manifest(&self.root)?;
        for held in [
            current.manifest().source_owner.as_ref(),
            disk.as_ref().and_then(|m| m.source_owner.as_ref()),
        ]
        .into_iter()
        .flatten()
        {
            if Some(held) != proposed.as_ref() {
                return Err(
                    "source-managed index requires its owning source publication journal".into(),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn publication_is_uncertain(&self) -> bool {
        self.pending_publication
            .lock()
            .map(|pending| pending.is_some())
            .unwrap_or(true)
    }

    pub fn snapshot(&self) -> Arc<OpenedSegmentSet> {
        Arc::clone(&self.current.read().expect("segment catalog lock poisoned"))
    }

    /// Storage evidence only: the owner still has to activate a matching serving
    /// view. Keep publication fenced while the caller records its durable
    /// decision. A failed manifest sync requires reopening before recovery.
    pub(crate) fn with_durable_snapshot<T>(
        &self,
        record: impl FnOnce(&OpenedSegmentSet) -> Result<T, tonic::Status>,
    ) -> Result<T, tonic::Status> {
        let failure = |e: String| tonic::Status::failed_precondition(e);
        let _update = self
            .update
            .lock()
            .map_err(|_| failure("segment update lock poisoned".into()))?;
        let pending = self
            .pending_publication
            .lock()
            .map_err(|_| failure("segment publication lock poisoned".into()))?;
        if pending.is_some() {
            return Err(failure(
                "segment publication is uncertain; reopen before recording a durable decision"
                    .into(),
            ));
        }
        let current = self
            .current
            .read()
            .map_err(|_| failure("segment catalog lock poisoned".into()))?;
        let disk = Self::read_manifest(&self.root).map_err(failure)?;
        if disk.as_ref().unwrap_or(&SegmentSetManifest::default()) != current.manifest() {
            return Err(failure(
                "durable manifest differs from the held catalog; reopen for recovery".into(),
            ));
        }
        if disk.is_some() {
            std::fs::File::open(Self::manifest_path(&self.root))
                .and_then(|file| file.sync_all())
                .map_err(|e| tonic::Status::internal(format!("sync projection manifest: {e}")))?;
        }
        std::fs::File::open(&self.root)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| tonic::Status::internal(format!("sync projection directory: {e}")))?;
        // A fresh catalog creates this directory beneath the application's
        // existing container. Persist that link as well as the entries inside.
        let parent = self
            .root
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| tonic::Status::internal(format!("sync projection container: {e}")))?;
        record(&current)
    }

    /// The catalog root's set manifest as written on disk, `None` when the
    /// root has none yet.
    pub fn read_manifest(root: &Path) -> Result<Option<SegmentSetManifest>, String> {
        let path = root.join(SET_FILE);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)
            .map_err(|error| format!("read segment set {}: {error}", path.display()))?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| format!("parse segment set {}: {error}", path.display()))
    }

    /// The set manifest file inside a catalog root.
    pub fn manifest_path(root: &Path) -> PathBuf {
        root.join(SET_FILE)
    }

    /// The directory of segment `id` under a catalog root.
    pub fn segment_dir(root: &Path, id: &str) -> PathBuf {
        root.join("segments").join(id)
    }

    /// Record the integer column the catalog's sealed segments are cut
    /// by: the same atomic manifest publication a compaction uses, so a
    /// node opening the catalog reports the partition key a partitioned
    /// compaction would have left. `None` returns the catalog to the
    /// bucket layout's declaration.
    pub fn publish_partition_key(&self, key: Option<String>) -> Result<(), String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned".to_string())?;
        let current = self.snapshot();
        let mut manifest = current
            .published_manifest()
            .with_binding(current.binding())?;
        manifest.epoch = manifest
            .epoch
            .checked_add(1)
            .ok_or("segment catalog epoch overflow")?;
        manifest.partition_key = key;
        self.publish(manifest).map(|_| ())
    }

    /// Pin metadata by the same atomic manifest publication used for rows.
    /// No independently mutable binding file can outlive or lag its generation.
    pub fn publish_binding(
        &self,
        binding: &StoredBinding,
    ) -> Result<Arc<OpenedSegmentSet>, String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned".to_string())?;
        self.check_disk_identity()?;
        let current = self.snapshot();
        match current.binding() {
            Some(held) if held != binding => {
                return Err("segment generation is bound to another mapping".into())
            }
            None if !current.is_empty() => {
                return Err("cannot bind a populated unbound segment set".into())
            }
            _ => {}
        }
        if current.manifest().binding.is_some() {
            return Ok(current);
        }
        let mut manifest = current.published_manifest().with_binding(Some(binding))?;
        manifest.epoch = manifest
            .epoch
            .checked_add(1)
            .ok_or("segment catalog epoch overflow")?;
        self.publish(manifest)
    }

    pub fn append(&self, source: SegmentSource<'_>) -> Result<Arc<OpenedSegmentSet>, String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned".to_string())?;
        self.check_disk_identity()?;
        validate_segment_id(source.segment_id)?;
        if Self::segment_dir(&self.root, source.segment_id).exists() {
            let manifest = Self::read_manifest(&self.root)?.ok_or_else(|| {
                "existing staged segment is not published; recovery is required".to_string()
            })?;
            let held = manifest
                .segments
                .iter()
                .find(|segment| segment.segment_id == source.segment_id)
                .ok_or_else(|| {
                    "existing staged segment is not published; recovery is required".to_string()
                })?;
            verify_retry_source(&self.root, &source, held)?;
            // Reaffirm durability even if the first attempt failed after rename.
            // Opening reuses unchanged segments and validates any newly adopted ones.
            return self.publish(manifest);
        }
        let metadata = stage_segment(&self.root, source)?;
        let current = self.snapshot();
        let mut manifest = current.published_manifest();
        manifest.epoch = manifest
            .epoch
            .checked_add(1)
            .ok_or("segment catalog epoch overflow")?;
        manifest.segments.push(metadata.clone());
        manifest.segments.sort_by_key(|segment| segment.base_label);
        let published = self.publish(manifest);
        if published.is_err() {
            cleanup_staged(&self.root, std::slice::from_ref(&metadata));
        }
        published
    }
}
