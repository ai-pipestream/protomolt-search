//! Privately staged maintenance outputs and their fenced publication.

use super::*;

/// Private copied images held until the owning publisher commits or abandons
/// them. A durable intent retains them on ambiguous failure for reconciliation.
pub(crate) struct StagedMaintenance {
    before: Arc<OpenedSegmentSet>,
    after: Arc<OpenedSegmentSet>,
    outputs: Vec<SegmentMetadata>,
    retain: std::cell::Cell<bool>,
}
impl StagedMaintenance {
    pub(crate) fn snapshot(&self) -> &Arc<OpenedSegmentSet> {
        &self.after
    }
    pub(crate) fn retain_for_recovery(&self) {
        self.retain.set(true);
    }
}
impl Drop for StagedMaintenance {
    fn drop(&mut self) {
        if !self.retain.get() {
            cleanup_staged(self.after.root(), &self.outputs);
        }
    }
}

impl SegmentCatalog {
    /// Copy and validate immutable outputs without holding the catalog update
    /// fence. The captured before view must still match at final publication.
    pub(crate) fn stage_maintenance(
        &self,
        before: Arc<OpenedSegmentSet>,
        sources: Vec<SegmentSource<'_>>,
    ) -> Result<StagedMaintenance, String> {
        if before.root() != self.root
            || before.manifest().source_owner.is_none()
            || before.generation_declaration().is_none()
        {
            return Err("maintenance requires this catalog's owned, declared generation".into());
        }
        let mut manifest = before.published_manifest();
        manifest.epoch = before
            .epoch()
            .checked_add(1)
            .ok_or("maintenance catalog epoch overflow")?;
        let mut ids: BTreeSet<&str> = before
            .manifest()
            .segments
            .iter()
            .map(|s| s.segment_id.as_str())
            .collect();
        for source in &sources {
            validate_segment_id(source.segment_id)?;
            if !ids.insert(source.segment_id) {
                return Err("maintenance requires fresh segment ids".into());
            }
        }
        let mut outputs = Vec::with_capacity(sources.len());
        let result = (|| {
            for source in sources {
                outputs.push(stage_segment(&self.root, source)?);
            }
            manifest.segments = outputs.clone();
            manifest.segments.sort_by_key(|segment| segment.base_label);
            manifest.update_retained_format();
            let mut end = 0;
            for segment in &manifest.segments {
                if segment.base_label != end {
                    return Err("maintenance requires contiguous rows beginning at zero".into());
                }
                end = segment.end_label_exclusive()?;
            }
            OpenedSegmentSet::open_manifest(self.root.clone(), manifest, self.load).map(Arc::new)
        })();
        match result {
            Ok(after) => Ok(StagedMaintenance {
                before,
                after,
                outputs,
                retain: std::cell::Cell::new(false),
            }),
            Err(error) => {
                cleanup_staged(&self.root, &outputs);
                Err(error)
            }
        }
    }

    /// Commit only the exact immutable views this owner already staged.
    /// The callback records intent and holds the final serving-state guard.
    pub(crate) fn commit_maintenance_prepared<T>(
        &self,
        staged: &StagedMaintenance,
        owner: &crate::pb::storage::SourceIndexOwner,
        prepare: impl FnOnce(&Arc<OpenedSegmentSet>) -> Result<T, String>,
    ) -> Result<(Arc<OpenedSegmentSet>, T), String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned")?;
        if self
            .pending_publication
            .lock()
            .map_err(|_| "segment publication lock poisoned")?
            .is_some()
        {
            return Err("segment publication is uncertain; reopen before maintenance".into());
        }
        self.check_source_owner(Some(owner))?;
        let current = self.snapshot();
        if staged.before.root() != self.root || current.manifest() != staged.before.manifest() {
            return Err("maintenance catalog changed during its private build".into());
        }
        let context = prepare(&staged.after)?;
        let published = self.publish_owned(staged.after.clone(), Some(owner))?;
        Ok((published, context))
    }
}
