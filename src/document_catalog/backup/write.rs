//! Write a captured backup as one complete local bundle.
use super::*;

/// Copy every pinned file to its bundle path, checking its length and
/// digest as it is written. Returns the directories created.
fn copy_pinned(files: &mut [PinnedFile], destination: &Path) -> Result<BTreeSet<PathBuf>, Status> {
    let mut directories = BTreeSet::new();
    directories.insert(destination.to_path_buf());
    let mut buffer = [0u8; 64 << 10];
    for pinned in files {
        let output = destination.join(&pinned.metadata.file);
        let directory = output.parent().expect("bundle artifact parent");
        std::fs::create_dir_all(directory).map_err(storage)?;
        let mut ancestor = directory;
        while ancestor != destination {
            directories.insert(ancestor.to_path_buf());
            ancestor = ancestor.parent().expect("generated bundle path");
        }
        let mut file = new_file(&output)?;
        let mut hash = sha256::Sha256::new();
        let mut count = 0u64;
        let mut copy = |input: &mut dyn Read| -> Result<(), Status> {
            loop {
                let n = input.read(&mut buffer).map_err(storage)?;
                if n == 0 {
                    break;
                }
                count = count.checked_add(n as u64).ok_or_else(exhausted)?;
                if count > pinned.metadata.bytes {
                    return Err(Status::data_loss("backup artifact grew after capture"));
                }
                file.write_all(&buffer[..n]).map_err(storage)?;
                hash.update(&buffer[..n]);
            }
            Ok(())
        };
        match &mut pinned.data {
            Data::Bytes(bytes) => copy(&mut bytes.as_slice())?,
            Data::File(input) => {
                input.seek(SeekFrom::Start(0)).map_err(storage)?;
                copy(input)?;
            }
        }
        if count != pinned.metadata.bytes
            || sha256::to_hex(&hash.finalize()) != pinned.metadata.sha256
        {
            return Err(Status::data_loss(
                "backup artifact changed or failed its checksum",
            ));
        }
        file.sync_all().map_err(storage)?;
    }
    Ok(directories)
}

impl CapturedBackup<'_> {
    /// Write one complete local bundle in an exclusively created directory.
    /// The source checkpoint is copied from the captured transaction; artifacts
    /// are copied from held files, never reopened by their retired source paths.
    /// Failed writes remove only their own directory. A process crash may leave
    /// a partial directory; without a verified completion manifest it is invalid.
    pub fn write_to(mut self, destination: &Path) -> Result<SourceBackupManifest, Status> {
        let name = destination
            .file_name()
            .ok_or_else(|| fail("backup destination needs a directory name"))?;
        let resolved = std::fs::canonicalize(parent_directory(destination))
            .map_err(storage)?
            .join(name);
        if self
            .source_roots
            .iter()
            .any(|root| resolved.starts_with(root))
        {
            return Err(fail(
                "backup destination must be outside every live catalog",
            ));
        }
        let parent = File::open(parent_directory(destination)).map_err(storage)?;
        let mut owned = create_private_directory(destination, "backup destination already exists")?;
        self.source.verify_source_history(
            &destination.join(".source-audit.redb"),
            self.limits.source_batch_bytes as usize,
            self.limits.max_bytes,
        )?;
        let remaining = self
            .limits
            .max_bytes
            .checked_sub(self.bytes)
            .filter(|n| *n > 0)
            .ok_or_else(exhausted)?;
        let source = self.source.write_to(
            &destination.join(SOURCE),
            &crate::pb::storage::DocumentCatalogCheckpointLimits {
                batch_bytes: self.limits.source_batch_bytes,
                max_file_bytes: remaining,
            },
        )?;
        let mut total = self.bytes;
        charge(&mut total, source.bytes, self.limits.max_bytes)?;
        let mut manifest = self.manifest(source);
        manifest.manifest_sha256 = sha256::digest(&manifest.encode_to_vec()).to_vec();
        let completed = manifest.encode_to_vec();
        charge(&mut total, completed.len() as u64, self.limits.max_bytes)?;
        drop(self.source);
        let directories = copy_pinned(&mut self.files, destination)?;
        commit_bundle(destination, &directories, &completed, &parent)?;
        owned.keep = true;
        Ok(manifest)
    }
}
