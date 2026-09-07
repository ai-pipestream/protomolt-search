use super::*;

/// Positional references resolved against one held segment-set epoch. These are
/// storage locations, not public document identities or authorization grants.
#[derive(Debug, Clone)]
pub struct SegmentRowRetirement {
    pub segment_id: String,
    pub rows: Vec<u64>,
}

impl SegmentCatalog {
    /// Publish added segments and retire old rows in one manifest swap. Supports
    /// replacements with a different row count, including no new rows.
    ///
    /// The epoch check and publication serialize through this catalog's shared
    /// update lock. The owner must still fence independent writers to the root.
    /// This primitive does not certify source acceptance or issue write receipts.
    pub fn commit_rows(
        &self,
        expected_epoch: u64,
        retirements: &[SegmentRowRetirement],
        sources: Vec<SegmentSource<'_>>,
    ) -> Result<Arc<OpenedSegmentSet>, String> {
        self.commit_rows_prepared(expected_epoch, retirements, sources, |_| Ok(()))
            .map(|(snapshot, ())| snapshot)
    }

    /// Prepare the complete consumer view before publishing the manifest. The
    /// callback runs after artifact validation while the catalog update lock is
    /// held, and must not mutate this catalog recursively. Its returned context
    /// stays alive across the manifest commit, so a consumer can hold its final
    /// visibility fence until it installs the already-prepared view.
    pub(crate) fn commit_rows_prepared<T>(
        &self,
        expected_epoch: u64,
        retirements: &[SegmentRowRetirement],
        sources: Vec<SegmentSource<'_>>,
        prepare: impl FnOnce(&Arc<OpenedSegmentSet>) -> Result<T, String>,
    ) -> Result<(Arc<OpenedSegmentSet>, T), String> {
        self.commit_row_transaction(expected_epoch, retirements, sources, None, None, prepare)
    }

    /// Source versions may produce zero rows. Still commit their epoch and
    /// reviewed binding, with the source journal prepared under this fence.
    pub(crate) fn commit_projection_prepared<T>(
        &self,
        expected_epoch: u64,
        retirements: &[SegmentRowRetirement],
        sources: Vec<SegmentSource<'_>>,
        binding: Option<&StoredBinding>,
        owner: &crate::pb::storage::SourceIndexOwner,
        prepare: impl FnOnce(&Arc<OpenedSegmentSet>) -> Result<T, String>,
    ) -> Result<(Arc<OpenedSegmentSet>, T), String> {
        self.commit_row_transaction(
            expected_epoch,
            retirements,
            sources,
            binding,
            Some(owner),
            prepare,
        )
    }

    fn commit_row_transaction<T>(
        &self,
        expected_epoch: u64,
        retirements: &[SegmentRowRetirement],
        sources: Vec<SegmentSource<'_>>,
        binding: Option<&StoredBinding>,
        owner: Option<&crate::pb::storage::SourceIndexOwner>,
        prepare: impl FnOnce(&Arc<OpenedSegmentSet>) -> Result<T, String>,
    ) -> Result<(Arc<OpenedSegmentSet>, T), String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned".to_string())?;
        if self
            .pending_publication
            .lock()
            .map_err(|_| "segment publication lock poisoned".to_string())?
            .is_some()
        {
            return Err(
                "segment publication is uncertain; reopen for recovery before another row update"
                    .into(),
            );
        }
        let current = self.snapshot();
        self.check_source_owner(owner)?;
        if current.epoch() != expected_epoch {
            return Err(format!(
                "row update expected epoch {expected_epoch}, current epoch is {}",
                current.epoch()
            ));
        }
        let epoch = expected_epoch
            .checked_add(1)
            .ok_or("segment catalog epoch overflow")?;
        if owner.is_none() && retirements.is_empty() && sources.is_empty() {
            return Err("row update requires retirements or new segments".into());
        }
        if binding.is_some_and(|binding| current.binding().is_some_and(|held| held != binding))
            || (binding.is_some() && current.binding().is_none() && !current.is_empty())
        {
            return Err("projection cannot change or adopt a populated mapped binding".into());
        }
        let mut seen = BTreeSet::new();
        let mut updates = Vec::new();
        // Resolve and validate all old locations before creating artifacts.
        for retirement in retirements {
            if !seen.insert(&retirement.segment_id) || retirement.rows.is_empty() {
                return Err("row update repeats a segment or has an empty retirement".into());
            }
            let index = current
                .segments
                .iter()
                .position(|segment| segment.metadata.segment_id == retirement.segment_id)
                .ok_or_else(|| {
                    format!(
                        "row update names unknown segment {:?}",
                        retirement.segment_id
                    )
                })?;
            let held = &current.segments[index];
            let mut rows = BTreeSet::new();
            for &row in &retirement.rows {
                if row >= held.metadata.rows || usize::try_from(row).is_err() || !rows.insert(row) {
                    return Err(format!(
                        "row update has an invalid or duplicate row in {:?}",
                        retirement.segment_id
                    ));
                }
            }
            updates.push((index, &retirement.rows));
        }
        let mut ids: BTreeSet<&str> = current
            .manifest
            .segments
            .iter()
            .map(|segment| segment.segment_id.as_str())
            .collect();
        let end = current
            .manifest
            .segments
            .last()
            .map(SegmentMetadata::end_label_exclusive)
            .transpose()?
            .unwrap_or(0);
        for source in &sources {
            validate_segment_id(source.segment_id)?;
            if !ids.insert(source.segment_id) || source.base_label < end {
                return Err(
                    "row update requires fresh segment ids and ranges after the existing rows"
                        .into(),
                );
            }
        }
        let mut manifest = current.published_manifest();
        manifest.source_owner = owner.map(SegmentSourceOwner::encode).transpose()?;
        manifest = manifest.with_binding(binding.or(current.binding()))?;
        manifest.epoch = epoch;
        let mut outputs = Vec::new();
        let mut overlays = Vec::new();
        let result = (|| {
            for (index, rows) in updates {
                let mut live = current.segments[index].live_docs.clone();
                for &row in rows {
                    live.delete(row as usize);
                }
                let metadata = &mut manifest.segments[index];
                let (artifact, created) = write_overlay(&self.root, metadata, &live, epoch)?;
                if created {
                    overlays.push((metadata.segment_id.clone(), artifact.clone()));
                }
                metadata.live_docs = artifact;
                metadata.live_rows = metadata
                    .rows
                    .checked_sub(live.deleted_count())
                    .ok_or("retired row count exceeds segment")?;
            }
            for source in sources {
                outputs.push(stage_segment(&self.root, source)?);
            }
            manifest.segments.extend(outputs.iter().cloned());
            manifest.segments.sort_by_key(|segment| segment.base_label);
            let opened = Arc::new(OpenedSegmentSet::open_manifest_reusing(
                self.root.clone(),
                manifest,
                self.load,
                Some(&current),
            )?);
            let context = prepare(&opened)?;
            let published = self.publish_owned(opened, owner)?;
            Ok((published, context))
        })();
        if result.is_err() {
            cleanup_staged(&self.root, &outputs);
            cleanup_overlays(&self.root, &overlays);
        }
        result
    }
}

fn write_overlay(
    root: &Path,
    metadata: &SegmentMetadata,
    live: &LiveDocs,
    epoch: u64,
) -> Result<(SegmentArtifact, bool), String> {
    let directory = SegmentCatalog::segment_dir(root, &metadata.segment_id);
    let temporary = directory.join(format!(".retire-{epoch}-{}.bin", std::process::id()));
    let result = (|| {
        live.write(&temporary, metadata.rows)
            .map_err(|error| format!("stage retired rows: {error}"))?;
        let (bytes, sha256) = digest_file(&temporary)?;
        let artifact = SegmentArtifact {
            file: format!("live-docs-{sha256}.bin"),
            bytes,
            sha256,
        };
        let target = directory.join(&artifact.file);
        let created = if target.exists() {
            verify_artifact(&directory, &artifact)?;
            false
        } else {
            std::fs::rename(&temporary, &target)
                .map_err(|error| format!("publish retired bitmap: {error}"))?;
            true
        };
        std::fs::File::open(&directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("sync retired bitmap directory: {error}"))?;
        Ok((artifact, created))
    })();
    let _ = std::fs::remove_file(temporary);
    result
}

fn cleanup_overlays(root: &Path, overlays: &[(String, SegmentArtifact)]) {
    let manifest = match SegmentCatalog::read_manifest(root) {
        Ok(value) => value,
        Err(_) => return,
    };
    for (id, artifact) in overlays {
        if manifest.as_ref().is_some_and(|manifest| {
            manifest
                .segments
                .iter()
                .any(|segment| segment.segment_id == *id && segment.live_docs.file == artifact.file)
        }) {
            continue;
        }
        let _ = std::fs::remove_file(SegmentCatalog::segment_dir(root, id).join(&artifact.file));
    }
}

impl OpenedSegmentSet {
    /// Resolve every live sealed row for an exact document key, across versions,
    /// against this immutable snapshot. The caller must use this snapshot's
    /// epoch for commit_rows and supply its authorization and version policy.
    /// Exceeding max_rows refuses the whole resolution, never a partial update.
    pub fn document_retirements(
        &self,
        key: &[u8],
        max_rows: usize,
    ) -> Result<Vec<SegmentRowRetirement>, String> {
        if key.is_empty() || key.len() > 16 * 1024 || max_rows == 0 {
            return Err(
                "document retirement requires a valid key and a positive row budget".into(),
            );
        }
        let mut remaining = max_rows;
        let mut retirements = Vec::new();
        for segment in &self.segments {
            let mut rows = Vec::new();
            let complete = segment.bm25.visit_document_rows(key, &mut |row, _, _| {
                if segment.live_docs.is_deleted(row as usize) {
                    return true;
                }
                if remaining == 0 {
                    return false;
                }
                remaining -= 1;
                rows.push(u64::from(row));
                true
            });
            if !complete {
                return Err("document retirement exceeds the row budget".into());
            }
            if !rows.is_empty() {
                retirements.push(SegmentRowRetirement {
                    segment_id: segment.metadata.segment_id.clone(),
                    rows,
                });
            }
        }
        Ok(retirements)
    }
}
