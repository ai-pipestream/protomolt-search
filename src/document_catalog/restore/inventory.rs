//! Check a bundle's artifact inventory against its source checkpoint and
//! every index manifest it names.
use super::input::read_bounded;
use super::*;
use crate::pb::storage::{
    DocumentCatalogCheckpoint, ProjectionJournalState, SourceBackupIndex, SourceBackupLimits,
};
use crate::segments::SegmentMetadata;

fn metadata(
    root: &Directory,
    artifact: &SnapshotArtifact,
    total: &mut u64,
    maximum: u64,
) -> Result<Vec<u8>, Status> {
    charge(total, artifact.bytes, maximum)?;
    let bytes = read_bounded(root, &artifact.file, artifact.bytes.min(maximum))?;
    if bytes.len() as u64 != artifact.bytes || sha256::hex_digest(&bytes) != artifact.sha256 {
        return Err(damaged("index metadata differs from its inventory"));
    }
    Ok(bytes)
}

/// The inventory under audit: every listed artifact by path, the paths
/// the checkpoint and index manifests account for, and the metadata bytes
/// read so far against their budget.
struct Inventory<'a> {
    root: &'a Directory,
    entries: BTreeMap<&'a str, &'a SnapshotArtifact>,
    required: BTreeSet<String>,
    metadata_bytes: u64,
    metadata_limit: u64,
}

/// Every listed artifact by path. The list is sorted, unique, checksummed
/// and within the byte and file budgets.
fn listed_artifacts<'a>(
    manifest: &'a SourceBackupManifest,
    completion_bytes: u64,
    limits: &SourceBackupLimits,
) -> Result<BTreeMap<&'a str, &'a SnapshotArtifact>, Status> {
    let mut total = completion_bytes;
    let mut entries = BTreeMap::new();
    let mut last = "";
    for artifact in &manifest.artifacts {
        relative(&artifact.file)?;
        if artifact.file.as_str() <= last
            || !hash_valid(&artifact.sha256)
            || artifact.file == COMPLETION
        {
            return Err(damaged(
                "artifact inventory is not sorted, unique and checksummed",
            ));
        }
        last = &artifact.file;
        charge(&mut total, artifact.bytes, limits.max_bytes)?;
        entries.insert(artifact.file.as_str(), artifact);
    }
    if total > limits.max_bytes || entries.len() > limits.max_files as usize {
        return Err(exhausted());
    }
    Ok(entries)
}

/// The inventory lists the source database with the checkpoint's length
/// and digest.
fn check_source_entry(
    entries: &BTreeMap<&str, &SnapshotArtifact>,
    source: &DocumentCatalogCheckpoint,
) -> Result<(), Status> {
    let source_file = entries
        .get(SOURCE)
        .ok_or_else(|| damaged("inventory omits sources.redb"))?;
    let source_digest: &[u8; 32] = source
        .sha256
        .as_slice()
        .try_into()
        .map_err(|_| damaged("source checkpoint checksum has an invalid length"))?;
    if source_file.bytes != source.bytes || source_file.sha256 != sha256::to_hex(source_digest) {
        return Err(damaged(
            "source checkpoint checksum or length differs from its inventory",
        ));
    }
    Ok(())
}

pub(super) fn inventory(
    root: &Directory,
    manifest: &SourceBackupManifest,
    completion_bytes: u64,
    limits: &SourceBackupLimits,
) -> Result<(), Status> {
    let source = manifest.source.as_ref().expect("validated source");
    let entries = listed_artifacts(manifest, completion_bytes, limits)?;
    check_source_entry(&entries, source)?;
    let mut audit = Inventory {
        root,
        entries,
        required: BTreeSet::from([SOURCE.to_string()]),
        metadata_bytes: completion_bytes,
        metadata_limit: limits.metadata_bytes,
    };
    let mut prior_key: &[u8] = &[];
    for (ordinal, (index, state)) in manifest.indexes.iter().zip(&source.indexes).enumerate() {
        let directory = format!("indexes/{ordinal:08}/index.segments");
        if index.index_key.is_empty()
            || index.index_key.as_slice() <= prior_key
            || index.index_key != state.index_key
            || index.directory != directory
        {
            return Err(damaged(
                "index inventory differs from the ordered source journal",
            ));
        }
        prior_key = &index.index_key;
        audit.index(&directory, index, state, source)?;
    }
    if audit.required.len() != audit.entries.len()
        || audit
            .required
            .iter()
            .any(|name| !audit.entries.contains_key(name.as_str()))
    {
        return Err(damaged("inventory contains unreferenced artifacts"));
    }
    Ok(())
}

impl Inventory<'_> {
    /// Read the listed metadata file at `path` within the metadata budget.
    fn metadata(&mut self, path: &str, missing: &'static str) -> Result<Vec<u8>, Status> {
        let entry = self.entries.get(path).ok_or_else(|| damaged(missing))?;
        metadata(
            self.root,
            entry,
            &mut self.metadata_bytes,
            self.metadata_limit,
        )
    }

    /// One index: its manifest is the journal's committed one, owned by
    /// this source, and every segment it names is listed.
    fn index(
        &mut self,
        directory: &str,
        index: &SourceBackupIndex,
        state: &ProjectionJournalState,
        source: &DocumentCatalogCheckpoint,
    ) -> Result<(), Status> {
        let path = format!("{directory}/segments.json");
        let bytes = self.metadata(&path, "index manifest is missing")?;
        let set: SegmentSetManifest =
            serde_json::from_slice(&bytes).map_err(|e| damaged(format!("index manifest: {e}")))?;
        if serde_json::to_vec(&set).map_err(storage)? != bytes
            || sha256::digest(&bytes).as_slice() != state.committed_manifest_sha256
            || set.epoch != index.catalog_epoch
        {
            return Err(damaged("index manifest differs from its journal or epoch"));
        }
        let owner = crate::pb::storage::SourceIndexOwner {
            format_version: 1,
            history_id: source.header.as_ref().unwrap().history_id.clone(),
            index_key: index.index_key.clone(),
            collection: source.header.as_ref().unwrap().collection.clone(),
        };
        match &set.source_owner {
            Some(held) if held.decode().map_err(damaged)? == owner => {}
            None if state.committed_sequence == 0 && set.segments.is_empty() => {}
            _ => return Err(damaged("index manifest belongs to another source owner")),
        }
        self.required.insert(path);
        for segment in &set.segments {
            self.segment(directory, segment)?;
        }
        Ok(())
    }

    /// One segment: its metadata file equals the index manifest's entry and
    /// each artifact it names is listed once with the same size and digest.
    fn segment(&mut self, directory: &str, segment: &SegmentMetadata) -> Result<(), Status> {
        relative(&segment.segment_id)?;
        if segment.segment_id.contains('/') {
            return Err(damaged("segment id is not one component"));
        }
        let prefix = format!("{directory}/segments/{}", segment.segment_id);
        let path = format!("{prefix}/segment.json");
        if !self.required.insert(path.clone()) {
            return Err(damaged("duplicate segment metadata"));
        }
        let bytes = self.metadata(&path, "segment metadata is missing")?;
        if serde_json::to_vec(segment).map_err(storage)? != bytes {
            return Err(damaged("segment metadata differs from index manifest"));
        }
        for artifact in [
            Some(&segment.vector),
            Some(&segment.exact_vectors),
            Some(&segment.bm25),
            Some(&segment.live_docs),
            segment.multi_res.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if artifact.file.is_empty() {
                continue;
            }
            relative(&artifact.file)?;
            if artifact.file.contains('/') {
                return Err(damaged("segment artifact is not one component"));
            }
            let path = format!("{prefix}/{}", artifact.file);
            let entry = self
                .entries
                .get(path.as_str())
                .ok_or_else(|| damaged("segment artifact is missing"))?;
            if entry.bytes != artifact.bytes
                || entry.sha256 != artifact.sha256
                || !self.required.insert(path)
            {
                return Err(damaged(
                    "segment artifact has conflicting metadata or roles",
                ));
            }
        }
        Ok(())
    }
}
