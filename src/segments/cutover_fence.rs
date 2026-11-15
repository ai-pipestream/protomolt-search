//! Pin a caught-up compaction shadow to its fenced serving generation.

use super::*;

pub(crate) struct CatalogCutover<'a> {
    catalog: &'a SegmentCatalog,
}

impl SegmentCatalog {
    /// The caller must first fence mutations and validate WAL generation/catch-up.
    pub(crate) fn with_caught_up_shadow<T>(
        &self,
        live: &Self,
        serving: &OpenedSegmentSet,
        install: impl FnOnce(&CatalogCutover<'_>) -> T,
    ) -> Result<T, String> {
        if !Arc::ptr_eq(&self.update, &live.update) {
            return Err("compaction shadow and serving catalog have different roots".into());
        }
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned")?;
        if self.publication_is_uncertain() || live.publication_is_uncertain() {
            return Err("uncertain catalog publication cannot enter compaction cutover".into());
        }
        live.check_serving_snapshot(serving)?;
        let disk = Self::read_manifest(&self.root)?;
        if self.snapshot().manifest().source_owner.is_some()
            || live.snapshot().manifest().source_owner.is_some()
        {
            return Err(
                "source-managed index requires its owning source publication journal".into(),
            );
        }
        *self
            .expected_manifest
            .lock()
            .map_err(|_| "segment generation lock poisoned")? = disk;
        Ok(install(&CatalogCutover { catalog: self }))
    }
}

impl CatalogCutover<'_> {
    pub(crate) fn commit_current(&self, epoch: u64) -> Result<Arc<OpenedSegmentSet>, String> {
        self.catalog.commit_current_locked(epoch)
    }

    pub(crate) fn with_snapshot<T>(
        &self,
        catalog: &SegmentCatalog,
        set: &OpenedSegmentSet,
        apply: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        if !Arc::ptr_eq(&catalog.current, &self.catalog.current) {
            return Err("cutover snapshot belongs to another catalog".into());
        }
        self.catalog.check_serving_snapshot(set)?;
        apply()
    }
}
