//! Durable acceptance of one assigned replay stream, independent of index rows.
//! This local API grants no network authority and never reports search visibility.

use std::fs::{File, OpenOptions};
use std::path::Path;

use prost::Message;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use tonic::Status;

use crate::pb::storage::ReplayJournalHeader;
#[cfg(any(test, feature = "tls"))]
use crate::pb::ReplayJournalReceipt;
use crate::pb::{ReplayAdmissionPolicy, ReplayJournalFrame, ReplayStreamBinding};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
const FRAMES: TableDefinition<u64, &[u8]> = TableDefinition::new("frames");
const FORMAT: u32 = 3;
/// The admission record of a journal no authority has decided on yet.
const NO_DECISION: &[u8] = &[];

mod admission;
mod authority;
mod frame;
mod read;
pub use authority::{ReplayAuthorityRefresh, ReplayAuthoritySession, MAX_AUTHORITY_VALIDITY};
pub use frame::seal_frame;
use frame::validate_frame;
pub const MAX_FRAME_BYTES: usize = 64 << 20;

fn storage(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("replay journal: {error}"))
}
fn decode<T: Message + Default>(bytes: &[u8]) -> Result<T, Status> {
    let value = T::decode(bytes).map_err(|e| Status::data_loss(format!("replay journal: {e}")))?;
    if value.encode_to_vec() != bytes {
        return Err(Status::data_loss(
            "replay journal requires canonical supported records",
        ));
    }
    Ok(value)
}
fn hash(domain: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut hash = crate::sha256::Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    hash.finalize().to_vec()
}

/// Validate the immutable assignment shape. An external authority must also
/// authorize its peer, source, target, epochs and current revision before use.
pub fn binding_digest(binding: &ReplayStreamBinding) -> Result<Vec<u8>, Status> {
    if binding.contract_version != 1 {
        return Err(Status::invalid_argument(
            "unsupported replay binding contract version",
        ));
    }
    crate::collections::validate_name(&binding.workspace).map_err(Status::invalid_argument)?;
    if !binding.collection.is_empty() {
        crate::collections::validate_name(&binding.collection).map_err(Status::invalid_argument)?;
    }
    for name in [&binding.source_shard_id, &binding.target_shard_id] {
        if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
            return Err(Status::invalid_argument(
                "replay shard identity must contain 1..256 non-control bytes",
            ));
        }
    }
    if binding.source_history_id.len() != 16
        || binding.target_history_id.len() != 16
        || binding.assignment_id.len() != 16
        || binding.baseline_sha256.len() != 32
        || binding.index_contract_sha256.len() != 32
        || binding.source_write_epoch == 0
        || binding.target_write_epoch == 0
        || binding.topology_revision == 0
    {
        return Err(Status::invalid_argument("replay binding needs history/assignment identities, baseline and contract digests, and nonzero fencing epochs/revision"));
    }
    Ok(hash(
        b"protomolt-search.replay-binding.v1\0",
        &binding.encode_to_vec(),
    ))
}

/// Exclusive durable receiver history. Keep it separate from the index until
/// the application path can bind each index mutation to this journal's proof.
pub struct ReplayJournal {
    database: Database,
    binding: ReplayStreamBinding,
    digest: Vec<u8>,
    authority: std::sync::Mutex<authority::Freshness>,
    #[cfg(test)]
    authority_clock: std::sync::Arc<dyn Fn() -> std::time::Instant + Send + Sync>,
    // Drop the database before releasing the portable exclusive file lock.
    _file_lock: File,
}
impl ReplayJournal {
    pub fn create(path: &Path, binding: ReplayStreamBinding) -> Result<Self, Status> {
        Self::open_file(path, binding, true)
    }
    pub fn open(path: &Path, binding: ReplayStreamBinding) -> Result<Self, Status> {
        Self::open_file(path, binding, false)
    }
    fn open_file(path: &Path, binding: ReplayStreamBinding, new: bool) -> Result<Self, Status> {
        let digest = binding_digest(&binding)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let directory = File::open(parent).map_err(storage)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(new)
            .open(path)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    Status::already_exists("replay journal already exists")
                } else {
                    storage(e)
                }
            })?;
        file.try_lock().map_err(|e| {
            Status::failed_precondition(format!("exclusive replay journal lock unavailable: {e}"))
        })?;
        if !new && file.metadata().map_err(storage)?.len() == 0 {
            return Err(Status::data_loss("existing replay journal is empty"));
        }
        let mut builder = Database::builder();
        builder.set_cache_size(8 << 20);
        let backend = redb::backends::FileBackend::new(file.try_clone().map_err(storage)?)
            .map_err(storage)?;
        let database = builder.create_with_backend(backend).map_err(storage)?;
        let journal = Self {
            database,
            binding,
            digest,
            authority: Default::default(),
            #[cfg(test)]
            authority_clock: std::sync::Arc::new(std::time::Instant::now),
            _file_lock: file,
        };
        if new {
            let mut tx = journal.database.begin_write().map_err(storage)?;
            tx.set_durability(Durability::Immediate).map_err(storage)?;
            {
                let mut meta = tx.open_table(META).map_err(storage)?;
                let header = ReplayJournalHeader {
                    format_version: FORMAT,
                    binding: Some(journal.binding.clone()),
                    binding_sha256: journal.digest.clone(),
                    accepted_sequence: 0,
                    source_clock: journal.binding.baseline_clock,
                    head_sha256: journal.digest.clone(),
                    admission_fenced: false,
                }
                .encode_to_vec();
                meta.insert("header", header.as_slice()).map_err(storage)?;
                meta.insert("admission", NO_DECISION).map_err(storage)?;
            }
            tx.open_table(FRAMES).map_err(storage)?;
            tx.commit().map_err(storage)?;
            directory.sync_all().map_err(storage)?;
        } else {
            journal.head()?;
        }
        Ok(journal)
    }

    fn checked_header(
        &self,
        bytes: &[u8],
        frames: &impl ReadableTable<u64, &'static [u8]>,
    ) -> Result<ReplayJournalHeader, Status> {
        let header: ReplayJournalHeader = decode(bytes)?;
        if header.format_version != FORMAT
            || header.binding.as_ref() != Some(&self.binding)
            || header.binding_sha256 != self.digest
        {
            return Err(Status::failed_precondition(
                "replay journal format or immutable assignment differs",
            ));
        }
        if frames.len().map_err(storage)? != header.accepted_sequence {
            return Err(Status::data_loss(
                "replay journal frame count differs from its accepted tip",
            ));
        }
        if header.accepted_sequence == 0 {
            if header.source_clock != self.binding.baseline_clock
                || header.head_sha256 != self.digest
            {
                return Err(Status::data_loss(
                    "empty replay journal differs from its baseline",
                ));
            }
        } else {
            let frame = self.read_frame(frames, header.accepted_sequence)?;
            if frame.source_clock != header.source_clock
                || frame.sha256 != header.head_sha256
                || header.source_clock <= self.binding.baseline_clock
            {
                return Err(Status::data_loss(
                    "replay journal head differs from its last frame",
                ));
            }
        }
        Ok(header)
    }
    fn read_frame(
        &self,
        frames: &impl ReadableTable<u64, &'static [u8]>,
        sequence: u64,
    ) -> Result<ReplayJournalFrame, Status> {
        let bytes = frames
            .get(sequence)
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("replay journal frame is missing"))?;
        if bytes.value().len() > MAX_FRAME_BYTES {
            return Err(Status::data_loss("stored replay frame exceeds its bound"));
        }
        let frame: ReplayJournalFrame = decode(bytes.value())?;
        validate_frame(&self.digest, &frame)
            .map_err(|e| Status::data_loss(e.message().to_string()))?;
        if frame.sequence != sequence {
            return Err(Status::data_loss(
                "stored replay sequence differs from its key",
            ));
        }
        Ok(frame)
    }
    #[cfg(any(test, feature = "tls"))]
    fn receipt(&self, frame: &ReplayJournalFrame, replayed: bool) -> ReplayJournalReceipt {
        ReplayJournalReceipt {
            binding_sha256: self.digest.clone(),
            sequence: frame.sequence,
            source_clock: frame.source_clock,
            sha256: frame.sha256.clone(),
            accepted: true,
            durable: true,
            searchable: false,
            replayed,
        }
    }
    pub fn head(&self) -> Result<ReplayJournalHeader, Status> {
        let tx = self.database.begin_read().map_err(storage)?;
        let meta = tx.open_table(META).map_err(storage)?;
        let bytes = meta
            .get("header")
            .map_err(storage)?
            .ok_or_else(|| Status::data_loss("replay journal header is missing"))?;
        let frames = tx.open_table(FRAMES).map_err(storage)?;
        let header = self.checked_header(bytes.value(), &frames)?;
        self.read_policy(&header, &meta)?;
        Ok(header)
    }
}

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod journal_recovery_tests;
#[cfg(test)]
mod journal_tests;
