//! Compare durable catalog identities before publishing a generation.

use super::*;
use std::collections::HashMap;
use std::sync::{OnceLock, Weak};

#[path = "cutover_fence.rs"]
mod cutover;
pub(crate) use cutover::CatalogCutover;

type CatalogLocks = HashMap<PathBuf, Weak<Mutex<()>>>;

fn canonical_lock_key(root: &Path) -> Result<PathBuf, String> {
    let mut ancestor = if root.is_absolute() {
        root.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|e| format!("catalog working directory: {e}"))?
            .join(root)
    };
    let mut missing = Vec::new();
    loop {
        match ancestor.canonicalize() {
            Ok(mut key) => {
                for component in missing.iter().rev() {
                    if component == ".." {
                        key.pop();
                    } else if component != "." {
                        key.push(component);
                    }
                }
                return Ok(key);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::symlink_metadata(&ancestor) {
                    Ok(_) => return Err(format!("catalog root {}: {error}", root.display())),
                    Err(missing) if missing.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(format!("catalog root {}: {error}", root.display())),
                }
                let component = ancestor.components().next_back().ok_or_else(|| {
                    format!("catalog root {} has no existing ancestor", root.display())
                })?;
                missing.push(component.as_os_str().to_owned());
                if !ancestor.pop() {
                    return Err(format!(
                        "catalog root {} has no existing ancestor",
                        root.display()
                    ));
                }
            }
            Err(error) => return Err(format!("catalog root {}: {error}", root.display())),
        }
    }
}

fn root_lock(root: &Path) -> Result<Arc<Mutex<()>>, String> {
    static LOCKS: OnceLock<Mutex<CatalogLocks>> = OnceLock::new();
    let root = canonical_lock_key(root)?;
    let mut locks = LOCKS
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| "catalog lock registry poisoned")?;
    locks.retain(|_, lock| lock.strong_count() != 0);
    if let Some(lock) = locks.get(&root).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(root, Arc::downgrade(&lock));
    Ok(lock)
}

impl SegmentCatalog {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, String> {
        Self::open_with(root, VectorLoad::default())
    }

    pub fn open_with(root: impl Into<PathBuf>, load: VectorLoad) -> Result<Self, String> {
        let root = root.into();
        std::fs::create_dir_all(root.join("segments"))
            .map_err(|error| format!("mkdir segment root {}: {error}", root.display()))?;
        let expected = Self::read_manifest(&root)?;
        let current = OpenedSegmentSet::open_with(root.clone(), load)?;
        Self::from_opened(root, current, expected, load)
    }

    /// Open a compaction shadow while pinning the live disk manifest separately.
    pub fn open_staged(
        root: impl Into<PathBuf>,
        manifest: SegmentSetManifest,
        load: VectorLoad,
    ) -> Result<Self, String> {
        let root = root.into();
        let expected = Self::read_manifest(&root)?;
        let current = OpenedSegmentSet::open_manifest(root.clone(), manifest, load)?;
        Self::from_opened(root, current, expected, load)
    }

    fn from_opened(
        root: PathBuf,
        current: OpenedSegmentSet,
        expected: Option<SegmentSetManifest>,
        load: VectorLoad,
    ) -> Result<Self, String> {
        if Self::read_manifest(&root)? != expected {
            return Err("catalog generation changed while opening; reopen for recovery".into());
        }
        let update = root_lock(&root)?;
        Ok(Self {
            root,
            current: Arc::new(RwLock::new(Arc::new(current))),
            update,
            pending_publication: Arc::new(Mutex::new(None)),
            expected_manifest: Arc::new(Mutex::new(expected)),
            load,
        })
    }

    /// Serialize a directory replacement with publications from every local handle.
    pub(crate) fn with_generation_lock<T>(
        root: &Path,
        replace: impl FnOnce() -> T,
    ) -> Result<T, String> {
        let lock = root_lock(root)?;
        let _guard = lock.lock().map_err(|_| "segment update lock poisoned")?;
        Ok(replace())
    }

    /// Refuse work from a handle superseded by another durable generation.
    pub(crate) fn check_current_generation(&self) -> Result<(), String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned")?;
        self.check_disk_identity()
    }

    pub(super) fn check_disk_identity(&self) -> Result<(), String> {
        let pending = self
            .pending_publication
            .lock()
            .map_err(|_| "segment publication lock poisoned")?;
        let expected = self
            .expected_manifest
            .lock()
            .map_err(|_| "segment generation lock poisoned")?;
        let disk = Self::read_manifest(&self.root)?;
        if disk != *expected && !(pending.is_some() && disk.as_ref() == pending.as_ref()) {
            return Err("catalog generation changed; reopen before publication".into());
        }
        Ok(())
    }

    pub(crate) fn with_current_snapshot<T>(
        &self,
        set: &OpenedSegmentSet,
        apply: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned")?;
        self.check_serving_snapshot(set)?;
        apply()
    }

    fn check_serving_snapshot(&self, set: &OpenedSegmentSet) -> Result<(), String> {
        self.check_disk_identity()?;
        // A catalog before its first publication has the default empty manifest.
        // The identity check above still rejects a missing published manifest.
        let durable = Self::read_manifest(&self.root)?.unwrap_or_default();
        if self.publication_is_uncertain()
            || set.root() != self.root
            || set.manifest() != self.snapshot().manifest()
            || &durable != set.manifest()
        {
            return Err("serving snapshot is not the committed catalog generation".into());
        }
        Ok(())
    }

    /// Publish a compaction shadow only over the disk generation it was opened on.
    pub fn commit_current(&self, epoch: u64) -> Result<Arc<OpenedSegmentSet>, String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned")?;
        self.commit_current_locked(epoch)
    }

    fn commit_current_locked(&self, epoch: u64) -> Result<Arc<OpenedSegmentSet>, String> {
        self.check_disk_identity()?;
        let current = self.snapshot();
        if epoch <= current.epoch() {
            return Err(format!(
                "compaction commit epoch {epoch} is not newer than the staged set's {}",
                current.epoch()
            ));
        }
        let mut manifest = current.published_manifest();
        manifest.epoch = epoch;
        self.publish(manifest)
    }

    pub(super) fn publish(
        &self,
        manifest: SegmentSetManifest,
    ) -> Result<Arc<OpenedSegmentSet>, String> {
        self.check_disk_identity()?;
        self.check_source_owner(None)?;
        if self
            .pending_publication
            .lock()
            .map_err(|_| "segment publication lock poisoned")?
            .as_ref()
            .is_some_and(|held| held != &manifest)
        {
            return Err(
                "segment publication is uncertain; retry the same manifest or reopen for recovery"
                    .into(),
            );
        }
        let current = self.snapshot();
        let opened = Arc::new(OpenedSegmentSet::open_manifest_reusing(
            self.root.clone(),
            manifest,
            self.load,
            Some(&current),
        )?);
        self.publish_owned(opened, None)
    }

    pub(super) fn publish_owned(
        &self,
        opened: Arc<OpenedSegmentSet>,
        owner: Option<&crate::pb::storage::SourceIndexOwner>,
    ) -> Result<Arc<OpenedSegmentSet>, String> {
        self.check_source_owner(owner)?;
        if opened.manifest().source_owner != owner.map(SegmentSourceOwner::encode).transpose()? {
            return Err("source-managed index owner cannot be added, changed or removed by a legacy publication".into());
        }
        let manifest = opened.manifest();
        let mut pending = self
            .pending_publication
            .lock()
            .map_err(|_| "segment publication lock poisoned")?;
        if pending.as_ref().is_some_and(|held| held != manifest) {
            return Err(
                "segment publication is uncertain; retry the same manifest or reopen for recovery"
                    .into(),
            );
        }
        let mut expected = self
            .expected_manifest
            .lock()
            .map_err(|_| "segment generation lock poisoned")?;
        let disk = Self::read_manifest(&self.root)?;
        if disk != *expected
            && !(pending.as_ref() == Some(manifest) && disk.as_ref() == Some(manifest))
        {
            return Err("catalog generation changed; reopen before publication".into());
        }
        if disk
            .as_ref()
            .is_some_and(|held| held != manifest && manifest.epoch <= held.epoch)
        {
            return Err("catalog publication must advance the durable generation epoch".into());
        }
        let mut current = self
            .current
            .write()
            .map_err(|_| "segment catalog lock poisoned")?;
        if let Err(error) = write_json_atomic(&self.root.join(SET_FILE), manifest) {
            *pending = Some(manifest.clone());
            return Err(error);
        }
        *expected = Some(manifest.clone());
        *current = Arc::clone(&opened);
        *pending = None;
        Ok(opened)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virgin_catalog_is_current_but_a_missing_publication_is_not() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "catalog-initial-publication-{}-{nonce}",
            std::process::id()
        ));
        let catalog = SegmentCatalog::open(&root).unwrap();
        assert!(SegmentCatalog::read_manifest(&root).unwrap().is_none());
        catalog
            .with_current_snapshot(&catalog.snapshot(), || Ok(()))
            .unwrap();
        let published = catalog.commit_current(1).unwrap();
        catalog
            .with_current_snapshot(&published, || Ok(()))
            .unwrap();
        std::fs::remove_file(SegmentCatalog::manifest_path(&root)).unwrap();
        let error = catalog
            .with_current_snapshot(&published, || Ok(()))
            .unwrap_err();
        assert!(error.contains("catalog generation changed"), "{error}");
        std::fs::remove_dir_all(root).unwrap();
    }
}
