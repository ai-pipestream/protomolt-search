//! Contiguous WAL append records coalesced into one replica stream.
//!
//! The primary logs one record per appended document, so a record-at-a-time
//! catch-up pays one RPC per document. Consecutive append records of one
//! kind are contiguous by id, and the append RPCs are streams, so a run of
//! them travels as a single stream that the replica answers with one
//! `(first_id, added)` pair. A keyed run carries one stable routing key per
//! row and never mixes with unkeyed rows, so the receiver assigns every key
//! to the row it was logged with.

use prost::Message;

use crate::pb::{AddDocumentsRequest, AddVectorsRequest};

use crate::replication_wire::{encoded_key_bytes, MAX_STREAM_KEY_BYTES};

/// Rows one coalesced stream carries at most.
pub(super) const MAX_STREAM_ROWS: u64 = 512;

/// Encoded request bytes one coalesced stream carries at most. A single
/// record over the cap still travels, alone.
pub(super) const MAX_STREAM_BYTES: usize = 8 << 20;

/// Admitted rows of one record, or of a slice of one, offered to a run:
/// `rows` rows as `requests`, under a document contract of `required`,
/// with one key per row or none at all.
pub(super) struct Piece<T> {
    pub(super) rows: u64,
    pub(super) required: u32,
    pub(super) requests: Vec<T>,
    pub(super) keys: Vec<Vec<u8>>,
}

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
    /// One stable routing key per row, in row order; empty for an unkeyed run.
    pub(super) keys: Vec<Vec<u8>>,
    key_bytes: usize,
}

impl<T: Message> Run<T> {
    /// A run of one piece starting at `first_id`.
    pub(super) fn of(first_id: u64, piece: Piece<T>) -> Self {
        Self {
            first_id,
            rows: piece.rows,
            required: piece.required,
            bytes: encoded_bytes(&piece.requests),
            requests: piece.requests,
            key_bytes: encoded_key_bytes(&piece.keys),
            keys: piece.keys,
        }
    }

    /// Append the next contiguous piece when it fits: the same contract
    /// requirement, keyed exactly when the run is, and the stream stays
    /// within its row, byte and key caps. A piece that does not fit is
    /// handed back untouched.
    pub(super) fn extend(&mut self, piece: Piece<T>) -> Option<Piece<T>> {
        let bytes = encoded_bytes(&piece.requests);
        let key_bytes = encoded_key_bytes(&piece.keys);
        if piece.required != self.required
            || piece.keys.is_empty() != self.keys.is_empty()
            || self.rows.saturating_add(piece.rows) > MAX_STREAM_ROWS
            || self.bytes.saturating_add(bytes) > MAX_STREAM_BYTES
            || self.key_bytes.saturating_add(key_bytes) > MAX_STREAM_KEY_BYTES
        {
            return Some(piece);
        }
        self.rows = self.rows.saturating_add(piece.rows);
        self.bytes = self.bytes.saturating_add(bytes);
        self.key_bytes = self.key_bytes.saturating_add(key_bytes);
        self.requests.extend(piece.requests);
        self.keys.extend(piece.keys);
        None
    }
}

fn encoded_bytes<T: Message>(requests: &[T]) -> usize {
    requests
        .iter()
        .fold(0usize, |acc, r| acc.saturating_add(r.encoded_len()))
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

    fn piece(rows: u64, required: u32, text: &str) -> Piece<AddDocumentsRequest> {
        Piece {
            rows,
            required,
            requests: vec![document(text)],
            keys: Vec::new(),
        }
    }

    fn keyed(key: &[u8]) -> Piece<AddDocumentsRequest> {
        Piece {
            rows: 1,
            required: 0,
            requests: vec![document("k")],
            keys: vec![key.to_vec()],
        }
    }

    #[test]
    fn a_run_takes_contiguous_records_until_a_cap_or_a_contract_change() {
        let mut run = Run::of(40, piece(1, 2, "a"));
        assert!(run.extend(piece(1, 2, "b")).is_none());
        assert_eq!((run.first_id, run.rows, run.requests.len()), (40, 2, 2));
        let refused = run.extend(piece(1, 1, "c")).unwrap();
        assert_eq!(refused.requests, vec![document("c")]);
        assert!(run.extend(piece(MAX_STREAM_ROWS - 1, 2, "d")).is_some());
        assert!(run.extend(piece(MAX_STREAM_ROWS - 2, 2, "d")).is_none());
        assert_eq!(run.rows, MAX_STREAM_ROWS);
        assert!(run.extend(piece(1, 2, "e")).is_some());
        assert_eq!((run.rows, run.requests.len()), (MAX_STREAM_ROWS, 3));
    }

    #[test]
    fn a_record_over_the_byte_cap_travels_alone() {
        let big = |rows| Piece {
            rows,
            required: 0,
            requests: vec![document(&"x".repeat(MAX_STREAM_BYTES))],
            keys: Vec::new(),
        };
        let mut run = Run::of(0, big(1));
        assert_eq!(run.rows, 1);
        assert!(run.extend(piece(1, 0, "y")).is_some());
        let mut small = Run::of(0, piece(1, 0, "y"));
        assert!(small.extend(big(1)).is_some());
        assert_eq!(small.requests, vec![document("y")]);
    }

    #[test]
    fn keyed_rows_join_only_keyed_runs_and_keep_their_keys_in_order() {
        let mut run = Run::of(7, keyed(b"a"));
        assert!(run.extend(keyed(b"b")).is_none());
        assert!(run.extend(piece(1, 0, "plain")).is_some());
        assert_eq!(run.keys, [b"a".to_vec(), b"b".to_vec()]);
        let mut plain = Run::of(7, piece(1, 0, "plain"));
        let refused = plain.extend(keyed(b"c")).unwrap();
        assert_eq!(refused.keys, [b"c".to_vec()]);
        assert!(plain.keys.is_empty());
    }

    #[test]
    fn a_keyed_run_stops_at_the_key_byte_cap_and_the_row_cap() {
        let wide = vec![0xAB; MAX_STREAM_KEY_BYTES / 2];
        let mut run = Run::of(0, keyed(&wide));
        let refused = run.extend(keyed(&wide)).unwrap();
        assert_eq!(refused.keys, [wide]);
        assert_eq!((run.rows, run.keys.len()), (1, 1));
        let mut narrow = Run::of(0, keyed(b"k"));
        for _ in 1..MAX_STREAM_ROWS {
            assert!(narrow.extend(keyed(b"k")).is_none());
        }
        assert!(narrow.extend(keyed(b"k")).is_some());
        assert_eq!(narrow.keys.len() as u64, MAX_STREAM_ROWS);
    }
}
