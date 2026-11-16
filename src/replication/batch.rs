//! Contiguous WAL append records coalesced into one replica stream.
//!
//! The primary logs one record per appended document, so a record-at-a-time
//! catch-up pays one RPC per document. Consecutive unkeyed append records
//! of one kind are contiguous by id, and the append RPCs are streams, so a
//! run of them travels as a single stream that the replica answers with one
//! `(first_id, added)` pair. A record carrying stable routing keys never
//! joins a run: the receiver takes one row per keyed stream.

use prost::Message;

use crate::pb::{AddDocumentsRequest, AddVectorsRequest};

/// Rows one coalesced stream carries at most.
pub(super) const MAX_STREAM_ROWS: u64 = 512;

/// Encoded request bytes one coalesced stream carries at most. A single
/// record over the cap still travels, alone.
pub(super) const MAX_STREAM_BYTES: usize = 8 << 20;

/// Append requests of one kind, contiguous from `first_id`, waiting to
/// share one stream.
pub(super) struct Run<T> {
    pub(super) first_id: u64,
    pub(super) rows: u64,
    /// The document contract version every record of the run requires;
    /// zero for vectors, whose contract is checked before a record is held.
    pub(super) required: u32,
    bytes: usize,
    pub(super) requests: Vec<T>,
}

impl<T: Message> Run<T> {
    /// A run of one record: `requests` holding `rows` rows from `first_id`.
    pub(super) fn of(first_id: u64, rows: u64, required: u32, requests: Vec<T>) -> Self {
        Self {
            first_id,
            rows,
            required,
            bytes: encoded_bytes(&requests),
            requests,
        }
    }

    /// Append the next contiguous record when it fits: the same contract
    /// requirement, and the stream stays within its row and byte caps.
    /// A record that does not fit is handed back untouched.
    pub(super) fn extend(&mut self, rows: u64, required: u32, requests: Vec<T>) -> Option<Vec<T>> {
        let bytes = encoded_bytes(&requests);
        if required != self.required
            || self.rows + rows > MAX_STREAM_ROWS
            || self.bytes + bytes > MAX_STREAM_BYTES
        {
            return Some(requests);
        }
        self.rows += rows;
        self.bytes += bytes;
        self.requests.extend(requests);
        None
    }
}

fn encoded_bytes<T: Message>(requests: &[T]) -> usize {
    requests.iter().map(Message::encoded_len).sum()
}

/// The append run a catch-up holds between records. Only one kind is held
/// at a time, so documents and vectors reach the replica in source order.
pub(super) enum Held {
    Nothing,
    Documents(Run<AddDocumentsRequest>),
    Vectors(Run<AddVectorsRequest>),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document(text: &str) -> AddDocumentsRequest {
        AddDocumentsRequest {
            text: text.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_run_takes_contiguous_records_until_a_cap_or_a_contract_change() {
        let mut run = Run::of(40, 1, 2, vec![document("a")]);
        assert!(run.extend(1, 2, vec![document("b")]).is_none());
        assert_eq!((run.first_id, run.rows, run.requests.len()), (40, 2, 2));
        let refused = run.extend(1, 1, vec![document("c")]).unwrap();
        assert_eq!(refused, vec![document("c")]);
        assert!(run
            .extend(MAX_STREAM_ROWS - 1, 2, vec![document("d")])
            .is_some());
        assert!(run
            .extend(MAX_STREAM_ROWS - 2, 2, vec![document("d")])
            .is_none());
        assert_eq!(run.rows, MAX_STREAM_ROWS);
        assert!(run.extend(1, 2, vec![document("e")]).is_some());
        assert_eq!((run.rows, run.requests.len()), (MAX_STREAM_ROWS, 3));
    }

    #[test]
    fn a_record_over_the_byte_cap_travels_alone() {
        let big = document(&"x".repeat(MAX_STREAM_BYTES));
        let mut run = Run::of(0, 1, 0, vec![big]);
        assert_eq!(run.rows, 1);
        assert!(run.extend(1, 0, vec![document("y")]).is_some());
        let mut small = Run::of(0, 1, 0, vec![document("y")]);
        let big = document(&"x".repeat(MAX_STREAM_BYTES));
        assert!(small.extend(1, 0, vec![big]).is_some());
        assert_eq!(small.requests, vec![document("y")]);
    }
}
