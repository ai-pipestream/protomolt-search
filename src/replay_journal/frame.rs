//! The frame chain: digests, shape validation, and the sender's seal.

use prost::Message;
use tonic::Status;

use super::{binding_digest, MAX_FRAME_BYTES};
use crate::pb::{ReplayJournalFrame, ReplayStreamBinding};

fn frame_digest(binding: &[u8], frame: &ReplayJournalFrame) -> Vec<u8> {
    let mut unsigned = frame.clone();
    unsigned.sha256.clear();
    let mut hash = crate::sha256::Sha256::new();
    hash.update(b"protomolt-search.replay-frame.v1\0");
    hash.update(binding);
    hash.update(&unsigned.encode_to_vec());
    hash.finalize().to_vec()
}
pub(super) fn validate_frame(binding: &[u8], frame: &ReplayJournalFrame) -> Result<(), Status> {
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
