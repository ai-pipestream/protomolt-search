//! A pinned source read transaction for the source half of a coherent backup.
//! Index artifacts must be captured against these exact journal states before
//! the enclosing backup can publish a completion manifest.
use super::*;
use crate::pb::storage::{DocumentCatalogCheckpoint, DocumentCatalogCheckpointLimits};
use redb::{ReadTransaction, TableHandle};
use std::io::{Read, Seek, SeekFrom};

mod audit;

pub(super) type BinaryTable = TableDefinition<'static, &'static [u8], &'static [u8]>;

/// A point-in-time source view. Acceptance and publication can continue while
/// this exists; redb retains old pages until the view is dropped. The eventual
/// bundle owner must capture every referenced index against `metadata()` and
/// keep this view alive until its database copy is complete.
pub struct CatalogCheckpoint<'a> {
    read: ReadTransaction,
    metadata: DocumentCatalogCheckpoint,
    binary_tables: Vec<BinaryTable>,
    // Retain the owner's explicit file lock as well as redb's read snapshot.
    _source: Option<&'a DocumentCatalog>,
}

impl DocumentCatalog {
    /// Pin all accepted versions, retry decisions and current journal states in
    /// one read transaction. Pending decisions refuse capture; resolve them at
    /// their owner first. This never repairs or advances the live journal.
    pub fn capture_checkpoint(
        &self,
        max_metadata_bytes: usize,
    ) -> Result<CatalogCheckpoint<'_>, Status> {
        if !self.durable {
            return Err(Status::failed_precondition(
                "catalog checkpoint requires durable source history",
            ));
        }
        CatalogCheckpoint::from_read(
            self.database.begin_read().map_err(storage)?,
            max_metadata_bytes,
            Some(self),
        )
    }
}

impl<'a> CatalogCheckpoint<'a> {
    // A restore uses a held read-only database in its private staging directory.
    pub(super) fn from_read(
        read: ReadTransaction,
        max_metadata_bytes: usize,
        source: Option<&'a DocumentCatalog>,
    ) -> Result<Self, Status> {
        if max_metadata_bytes == 0 || max_metadata_bytes > 64 << 20 {
            return Err(Status::invalid_argument(
                "checkpoint metadata budget must be 1..64 MiB",
            ));
        }
        let header: DocumentCatalogHeader = {
            let meta = read.open_table(META).map_err(storage)?;
            let record = meta
                .get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("catalog header missing"))?;
            if record.value().len() > max_metadata_bytes {
                return Err(Status::resource_exhausted(
                    "checkpoint metadata budget exceeded",
                ));
            }
            decode_header(record.value())?
        };
        validate_current_header(&header)?;
        actors::validate_read_counts(&read, &header)?;
        let (indexes, journal_tables) =
            publication::checkpoint_states(&read, &header, max_metadata_bytes)?;
        let metadata = DocumentCatalogCheckpoint {
            format_version: 1,
            header: Some(header),
            indexes,
            ..Default::default()
        };
        if metadata.encoded_len() > max_metadata_bytes {
            return Err(Status::resource_exhausted(
                "checkpoint metadata budget exceeded",
            ));
        }
        let mut binary_tables = vec![HEADS, VERSIONS, OPERATIONS, DESCRIPTORS, SOURCES];
        if matches!(
            metadata
                .header
                .as_ref()
                .expect("captured header")
                .format_version,
            ACCESS_CONTROLLED_FORMAT | MANAGED_FORMAT | ACTIVE_MANAGED_FORMAT
        ) {
            binary_tables.push(actors::OPERATIONS);
        }
        binary_tables.extend(journal_tables);
        let mut expected = vec![META.name().to_string(), CHANGES.name().to_string()];
        expected.extend(binary_tables.iter().map(|t| t.name().to_string()));
        expected.sort();
        let mut names: Vec<_> = read
            .list_tables()
            .map_err(storage)?
            .map(|t| t.name().to_string())
            .collect();
        names.sort();
        if names != expected
            || read
                .list_multimap_tables()
                .map_err(storage)?
                .next()
                .is_some()
        {
            return Err(Status::failed_precondition(
                "checkpoint refuses unknown or missing source tables",
            ));
        }
        Ok(CatalogCheckpoint {
            read,
            metadata,
            binary_tables,
            _source: source,
        })
    }
}

impl CatalogCheckpoint<'_> {
    pub(super) fn record_count(&self) -> Result<u64, Status> {
        let mut count = self
            .read
            .open_table(META)
            .map_err(storage)?
            .len()
            .map_err(storage)?;
        let mut add = |n: u64| -> Result<(), Status> {
            count = count
                .checked_add(n)
                .ok_or_else(|| Status::data_loss("checkpoint record count overflow"))?;
            Ok(())
        };
        add(self
            .read
            .open_table(CHANGES)
            .map_err(storage)?
            .len()
            .map_err(storage)?)?;
        for table in &self.binary_tables {
            add(self
                .read
                .open_table(*table)
                .map_err(storage)?
                .len()
                .map_err(storage)?)?;
        }
        Ok(count)
    }

    /// The exact source/index anchors that a complete backup must satisfy.
    /// The file checksum, byte count and record count are populated by `write_to`.
    pub fn metadata(&self) -> &DocumentCatalogCheckpoint {
        &self.metadata
    }

    /// Copy raw table records into a newly created database using bounded output
    /// transactions. No protobuf record is re-encoded, so persisted legacy retry
    /// bytes and unknown fields survive. The caller owns the containing private
    /// staging directory and must not expose this file as a complete backup.
    /// A failed copy removes only the file exclusively created by this call.
    pub fn write_to(
        &self,
        path: &Path,
        limits: &DocumentCatalogCheckpointLimits,
    ) -> Result<DocumentCatalogCheckpoint, Status> {
        if limits.batch_bytes == 0 || limits.batch_bytes > 64 << 20 || limits.max_file_bytes == 0 {
            return Err(Status::invalid_argument(
                "checkpoint needs batch_bytes 1..64 MiB and positive max_file_bytes",
            ));
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let directory = File::open(parent).map_err(storage)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        // Declare the guard before the file so errors close all handles before
        // removal, including on platforms that cannot unlink an open file.
        let mut owned = OwnedFile { path, keep: true };
        let file = options.open(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Status::already_exists("checkpoint destination already exists")
            } else {
                storage(error)
            }
        })?;
        owned.keep = false;
        file.try_lock().map_err(storage)?;
        let mut builder = Database::builder();
        builder.set_cache_size(CACHE_BYTES);
        let database = builder
            .create_file(file.try_clone().map_err(storage)?)
            .map_err(storage)?;
        let records = {
            let mut copy = Copier {
                database: &database,
                file: &file,
                limits,
                records: 0,
            };
            // Metadata is copied as raw bytes too. It is valid only after all other
            // tables have been copied and the enclosing bundle has been completed.
            copy.table(&self.read, META)?;
            copy.table(&self.read, CHANGES)?;
            for table in &self.binary_tables {
                copy.table(&self.read, *table)?;
            }
            copy.records
        };
        drop(database);
        file.sync_all().map_err(storage)?;
        let mut result = self.metadata.clone();
        result.bytes = file.metadata().map_err(storage)?.len();
        if result.bytes > limits.max_file_bytes {
            return Err(Status::resource_exhausted(
                "checkpoint database exceeds max_file_bytes",
            ));
        }
        result.records = records;
        let mut input = &file;
        input.seek(SeekFrom::Start(0)).map_err(storage)?;
        let mut hash = sha256::Sha256::new();
        let mut buffer = [0u8; 64 << 10];
        loop {
            let n = input.read(&mut buffer).map_err(storage)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        result.sha256 = hash.finalize().to_vec();
        directory.sync_all().map_err(storage)?;
        owned.keep = true;
        Ok(result)
    }
}

struct OwnedFile<'a> {
    path: &'a Path,
    keep: bool,
}
impl Drop for OwnedFile<'_> {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_file(self.path);
        }
    }
}

struct Copier<'a> {
    database: &'a Database,
    file: &'a File,
    limits: &'a DocumentCatalogCheckpointLimits,
    records: u64,
}
impl Copier<'_> {
    fn table<K: redb::Key + 'static>(
        &mut self,
        read: &ReadTransaction,
        definition: TableDefinition<K, &'static [u8]>,
    ) -> Result<(), Status> {
        let source = read.open_table(definition).map_err(storage)?;
        let mut rows = source.iter().map_err(storage)?.peekable();
        // Even an empty declared table must be retained.
        loop {
            let mut tx = self.database.begin_write().map_err(storage)?;
            tx.set_durability(Durability::Immediate).map_err(storage)?;
            {
                let mut target = tx.open_table(definition).map_err(storage)?;
                let mut bytes = 0u64;
                let mut batch_rows = 0usize;
                while let Some(row) = rows.peek() {
                    let (key, value) = row.as_ref().map_err(storage)?;
                    let charge = (K::as_bytes(&key.value()).as_ref().len() as u64)
                        .checked_add(value.value().len() as u64)
                        .and_then(|v| v.checked_add(64))
                        .ok_or_else(|| {
                            Status::resource_exhausted("checkpoint record size overflow")
                        })?;
                    if charge > self.limits.batch_bytes {
                        return Err(Status::resource_exhausted(
                            "checkpoint record exceeds batch_bytes",
                        ));
                    }
                    if bytes + charge > self.limits.batch_bytes || batch_rows == 65_536 {
                        break;
                    }
                    target.insert(key.value(), value.value()).map_err(storage)?;
                    self.records = self.records.checked_add(1).ok_or_else(|| {
                        Status::resource_exhausted("checkpoint record count overflow")
                    })?;
                    bytes += charge;
                    batch_rows += 1;
                    rows.next();
                }
            }
            tx.commit().map_err(storage)?;
            if self.file.metadata().map_err(storage)?.len() > self.limits.max_file_bytes {
                return Err(Status::resource_exhausted(
                    "checkpoint database exceeds max_file_bytes",
                ));
            }
            if rows.peek().is_none() {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
