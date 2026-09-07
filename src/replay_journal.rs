//! Durable acceptance of one assigned replay stream, independent of index rows.
//! This local API grants no network authority and never reports search visibility.

use std::fs::{File, OpenOptions};
use std::path::Path;

use prost::Message;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use tonic::Status;

use crate::pb::storage::ReplayJournalHeader;
use crate::pb::{
    ReadReplayJournalRequest, ReadReplayJournalResponse, ReplayAdmissionPolicy, ReplayJournalFrame,
    ReplayJournalReceipt, ReplayStreamBinding,
};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
const FRAMES: TableDefinition<u64, &[u8]> = TableDefinition::new("frames");
const FORMAT: u32 = 1;
const AUTHORIZED_FORMAT: u32 = 2;
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

fn frame_digest(binding: &[u8], frame: &ReplayJournalFrame) -> Vec<u8> {
    let mut unsigned = frame.clone();
    unsigned.sha256.clear();
    let mut hash = crate::sha256::Sha256::new();
    hash.update(b"protomolt-search.replay-frame.v1\0");
    hash.update(binding);
    hash.update(&unsigned.encode_to_vec());
    hash.finalize().to_vec()
}
fn validate_frame(binding: &[u8], frame: &ReplayJournalFrame) -> Result<(), Status> {
    if frame.encoded_len() > MAX_FRAME_BYTES {
        return Err(Status::resource_exhausted("replay frame exceeds 64 MiB"));
    }
    if frame.sequence == 0
        || frame.source_clock == 0
        || frame.previous_sha256.len() != 32
        || frame.sha256.len() != 32
        || frame.record.is_empty()
    {
        return Err(Status::invalid_argument(
            "replay frame requires sequence, source clock, record and 32-byte chain digests",
        ));
    }
    let record = crate::pb::wal::WalRecord::decode(frame.record.as_slice())
        .map_err(|e| Status::invalid_argument(format!("invalid replay WAL envelope: {e}")))?;
    if record.seq == 0 || record.clock != frame.source_clock || record.op.is_none() {
        return Err(Status::invalid_argument(
            "replay frame disagrees with its WAL record clock or has no operation",
        ));
    }
    if frame.sha256 != frame_digest(binding, frame) {
        return Err(Status::invalid_argument(
            "replay frame digest differs from its binding or contents",
        ));
    }
    Ok(())
}

/// Form the sender's frame without translating the original WAL bytes.
pub fn seal_frame(
    binding: &ReplayStreamBinding,
    sequence: u64,
    previous_sha256: Vec<u8>,
    record: Vec<u8>,
) -> Result<ReplayJournalFrame, Status> {
    if record.len() > MAX_FRAME_BYTES {
        return Err(Status::resource_exhausted("replay record exceeds 64 MiB"));
    }
    let decoded = crate::pb::wal::WalRecord::decode(record.as_slice())
        .map_err(|e| Status::invalid_argument(format!("invalid replay WAL envelope: {e}")))?;
    let binding = binding_digest(binding)?;
    let mut frame = ReplayJournalFrame {
        sequence,
        source_clock: decoded.clock,
        previous_sha256,
        record,
        sha256: Vec::new(),
    };
    frame.sha256 = frame_digest(&binding, &frame);
    validate_frame(&binding, &frame)?;
    Ok(frame)
}

/// Exclusive durable receiver history. Keep it separate from the index until
/// the application path can bind each index mutation to this journal's proof.
pub struct ReplayJournal {
    database: Database,
    binding: ReplayStreamBinding,
    digest: Vec<u8>,
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
        let database = builder
            .create_file(file.try_clone().map_err(storage)?)
            .map_err(storage)?;
        let journal = Self {
            database,
            binding,
            digest,
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
        if !matches!(header.format_version, FORMAT | AUTHORIZED_FORMAT)
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

    /// Atomically persist the frame and acceptance frontier. An exact retry
    /// returns its original proof even after later frames have been accepted.
    pub fn accept(&self, frame: &ReplayJournalFrame) -> Result<ReplayJournalReceipt, Status> {
        self.accept_inner(frame, None)
    }

    /// Admit an mTLS request using the verified leaf certificate supplied by
    /// tonic's transport. No header, bearer token or caller-supplied digest is
    /// an authenticated peer. The listener must verify its configured client CA.
    #[cfg(feature = "tls")]
    pub fn accept_authenticated(
        &self,
        request: &tonic::Request<ReplayJournalFrame>,
    ) -> Result<ReplayJournalReceipt, Status> {
        let certificates = request.peer_certs().ok_or_else(|| {
            Status::unauthenticated("replay requires a verified client certificate")
        })?;
        let certificate = certificates.first().ok_or_else(|| {
            Status::unauthenticated("replay requires a verified client certificate")
        })?;
        let fingerprint = hash(b"", certificate.as_ref());
        self.accept_inner(request.get_ref(), Some(&fingerprint))
    }

    fn accept_inner(
        &self,
        frame: &ReplayJournalFrame,
        peer: Option<&[u8]>,
    ) -> Result<ReplayJournalReceipt, Status> {
        validate_frame(&self.digest, frame)?;
        let encoded = frame.encode_to_vec();
        let mut tx = self.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let mut frames = tx.open_table(FRAMES).map_err(storage)?;
            let mut header = self.checked_header(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("replay journal header is missing"))?
                    .value(),
                &frames,
            )?;
            let policy = self.read_policy(&header, &meta)?;
            self.authorize_peer(policy.as_ref(), peer, header.admission_fenced)?;
            if frame.sequence <= header.accepted_sequence {
                let held = self.read_frame(&frames, frame.sequence)?;
                if held != *frame {
                    return Err(Status::already_exists(
                        "replay sequence already names different content",
                    ));
                }
                return Ok(self.receipt(frame, true));
            }
            if header.accepted_sequence.checked_add(1) != Some(frame.sequence)
                || frame.previous_sha256 != header.head_sha256
                || frame.source_clock <= header.source_clock
            {
                return Err(Status::failed_precondition("replay frame has a sequence gap, divergent chain or non-increasing source clock"));
            }
            frames
                .insert(frame.sequence, encoded.as_slice())
                .map_err(storage)?;
            header.accepted_sequence = frame.sequence;
            header.source_clock = frame.source_clock;
            header.head_sha256 = frame.sha256.clone();
            let header = header.encode_to_vec();
            meta.insert("header", header.as_slice()).map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        Ok(self.receipt(frame, false))
    }

    /// Install a decision from the trusted control publisher. There is no
    /// sender-facing route to this method. Revocation and frame acceptance
    /// serialize through the same durable write transaction.
    pub fn publish_authorization(&self, policy: &ReplayAdmissionPolicy) -> Result<(), Status> {
        Self::validate_policy(policy)?;
        if policy.binding_sha256 != self.digest {
            return Err(Status::failed_precondition(
                "replay policy names another assignment",
            ));
        }
        let mut tx = self.database.begin_write().map_err(storage)?;
        tx.set_durability(Durability::Immediate).map_err(storage)?;
        {
            let mut meta = tx.open_table(META).map_err(storage)?;
            let frames = tx.open_table(FRAMES).map_err(storage)?;
            let mut header = self.checked_header(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| Status::data_loss("replay journal header is missing"))?
                    .value(),
                &frames,
            )?;
            if let Some(held) = self.read_policy(&header, &meta)? {
                if held.authority_id != policy.authority_id || policy.revision < held.revision {
                    return Err(Status::failed_precondition(
                        "replay authority incarnation differs or policy revision regressed",
                    ));
                }
                if held.revision == policy.revision {
                    return if held == *policy {
                        Ok(())
                    } else {
                        Err(Status::already_exists(
                            "replay policy revision already names another decision",
                        ))
                    };
                }
            } else if header.accepted_sequence != 0 {
                return Err(Status::failed_precondition("cannot authorize a journal with unassigned accepted history; start a new assignment"));
            }
            if header.admission_fenced && policy.enabled && self.matches_live_assignment(policy) {
                return Err(Status::failed_precondition(
                    "replay assignment was permanently fenced; start a new assignment",
                ));
            }
            header.admission_fenced |= !self.matches_live_assignment(policy);
            header.format_version = AUTHORIZED_FORMAT;
            meta.insert("header", header.encode_to_vec().as_slice())
                .map_err(storage)?;
            meta.insert("admission", policy.encode_to_vec().as_slice())
                .map_err(storage)?;
        }
        tx.commit().map_err(storage)
    }

    fn validate_policy(policy: &ReplayAdmissionPolicy) -> Result<(), Status> {
        use crate::pb::NodeResidency;
        if policy.contract_version != 1
            || policy.authority_id.len() != 16
            || policy.revision == 0
            || policy.binding_sha256.len() != 32
            || policy.source_history_id.len() != 16
            || policy.target_history_id.len() != 16
            || policy.source_write_epoch == 0
            || policy.target_write_epoch == 0
            || policy.source_certificate_sha256.len() > 64
            || policy
                .source_certificate_sha256
                .iter()
                .any(|p| p.len() != 32)
        {
            return Err(Status::invalid_argument("invalid replay admission policy version, identities, digests, epochs or peer count"));
        }
        let mut peers = std::collections::BTreeSet::new();
        if policy
            .source_certificate_sha256
            .iter()
            .any(|p| !peers.insert(p))
        {
            return Err(Status::invalid_argument(
                "replay policy repeats a certificate fingerprint",
            ));
        }
        for residency in [policy.source_residency, policy.target_residency] {
            if !matches!(
                NodeResidency::try_from(residency),
                Ok(NodeResidency::Server | NodeResidency::Device)
            ) {
                return Err(Status::invalid_argument(
                    "replay policy requires explicit known source and target residency",
                ));
            }
        }
        Ok(())
    }

    fn read_policy(
        &self,
        header: &ReplayJournalHeader,
        meta: &impl ReadableTable<&'static str, &'static [u8]>,
    ) -> Result<Option<ReplayAdmissionPolicy>, Status> {
        let stored = meta.get("admission").map_err(storage)?;
        match (header.format_version, stored) {
            (FORMAT, None) if !header.admission_fenced => Ok(None),
            (AUTHORIZED_FORMAT, Some(stored)) => {
                // Bound allocation before protobuf decode, including tampered files.
                if stored.value().len() > 8192 {
                    return Err(Status::data_loss(
                        "stored replay admission policy exceeds its bound",
                    ));
                }
                let policy: ReplayAdmissionPolicy = decode(stored.value())?;
                Self::validate_policy(&policy)
                    .map_err(|e| Status::data_loss(e.message().to_string()))?;
                if policy.binding_sha256 != self.digest {
                    return Err(Status::data_loss(
                        "stored replay policy names another assignment",
                    ));
                }
                if !header.admission_fenced && !self.matches_live_assignment(&policy) {
                    return Err(Status::data_loss(
                        "replay policy fencing observation lacks its permanent fence",
                    ));
                }
                Ok(Some(policy))
            }
            _ => Err(Status::data_loss(
                "replay journal format and admission policy disagree",
            )),
        }
    }

    fn matches_live_assignment(&self, policy: &ReplayAdmissionPolicy) -> bool {
        use crate::pb::NodeResidency;
        policy.source_residency == NodeResidency::Server as i32
            && policy.target_residency == NodeResidency::Server as i32
            && policy.source_history_id == self.binding.source_history_id
            && policy.target_history_id == self.binding.target_history_id
            && policy.source_write_epoch == self.binding.source_write_epoch
            && policy.target_write_epoch == self.binding.target_write_epoch
    }

    fn authorize_peer(
        &self,
        policy: Option<&ReplayAdmissionPolicy>,
        peer: Option<&[u8]>,
        permanently_fenced: bool,
    ) -> Result<(), Status> {
        match (policy, peer) {
            (None, None) => Ok(()), // Explicit trusted local kernel, format 1 only.
            (None, Some(_)) => Err(Status::permission_denied(
                "replay assignment has no installed authority decision",
            )),
            (Some(_), None) => Err(Status::unauthenticated(
                "authorized replay journal requires a verified peer",
            )),
            (Some(policy), Some(peer)) => {
                if permanently_fenced
                    || !policy.enabled
                    || !policy.source_certificate_sha256.iter().any(|p| p == peer)
                    || !self.matches_live_assignment(policy)
                {
                    return Err(Status::permission_denied(
                        "replay peer or current assignment fencing state is not authorized",
                    ));
                }
                Ok(())
            }
        }
    }

    /// Bounded snapshot pagination. No record can exceed the caller's byte
    /// budget, including the first record; failure never advances the cursor.
    pub fn read(
        &self,
        request: &ReadReplayJournalRequest,
    ) -> Result<ReadReplayJournalResponse, Status> {
        if !(1..=1000).contains(&request.limit)
            || request.max_bytes == 0
            || request.max_bytes > MAX_FRAME_BYTES as u64
        {
            return Err(Status::invalid_argument(
                "replay reads require limit 1..1000 and max_bytes 1..64 MiB",
            ));
        }
        let tx = self.database.begin_read().map_err(storage)?;
        let meta = tx.open_table(META).map_err(storage)?;
        let frames = tx.open_table(FRAMES).map_err(storage)?;
        let header = self.checked_header(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("replay journal header is missing"))?
                .value(),
            &frames,
        )?;
        self.read_policy(&header, &meta)?;
        let through = request.through_sequence.unwrap_or(header.accepted_sequence);
        if request.after_sequence > through || through > header.accepted_sequence {
            return Err(Status::invalid_argument(
                "replay read cursor or fence exceeds the accepted prefix",
            ));
        }
        let (mut previous, mut clock) = if request.after_sequence == 0 {
            (self.digest.clone(), self.binding.baseline_clock)
        } else {
            let prior = self.read_frame(&frames, request.after_sequence)?;
            (prior.sha256, prior.source_clock)
        };
        let mut out = Vec::new();
        let mut bytes = 0u64;
        let mut next = request.after_sequence;
        while next < through && out.len() < request.limit as usize {
            let frame = self.read_frame(&frames, next + 1)?;
            if frame.previous_sha256 != previous || frame.source_clock <= clock {
                return Err(Status::data_loss(
                    "replay journal prefix has a divergent chain or source clock",
                ));
            }
            let size = frame.encoded_len() as u64;
            if size > request.max_bytes - bytes {
                if out.is_empty() {
                    return Err(Status::resource_exhausted(
                        "first replay frame exceeds the requested byte budget",
                    ));
                }
                break;
            }
            bytes += size;
            previous = frame.sha256.clone();
            clock = frame.source_clock;
            next = frame.sequence;
            out.push(frame);
        }
        Ok(ReadReplayJournalResponse {
            frames: out,
            through_sequence: through,
            next_sequence: next,
            complete: next == through,
        })
    }
}

#[cfg(test)]
mod admission_tests;
