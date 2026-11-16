//! What a replica advertises to a catch-up sender, and the metadata of the
//! catch-up streams that depend on it.
//!
//! A receiver reporting [`RECEIVER_VERSION`] in `HealthResponse.replication_version`
//! takes append streams of several keyed rows under
//! [`STABLE_KEYS_METADATA`](crate::replication_wire::STABLE_KEYS_METADATA) and document streams
//! marked [`WAL_REPLAY_METADATA`]. A receiver that predates either would
//! read such a stream as fresh, unkeyed ingest, so the sender refuses to
//! send one to it instead of guessing.

/// The replication contract this build's receiver accepts.
pub const RECEIVER_VERSION: u32 = 1;

/// Binary metadata entry naming the stable routing key of a stream that
/// carries exactly one row.
pub const STABLE_KEY_METADATA: &str = "x-protomolt-stable-key-bin";

/// Binary metadata entry marking a document stream as WAL replay. The
/// value is the little-endian `u64` global id of the stream's first row,
/// which must be the receiver's document tip when the stream opens.
///
/// A replayed document keeps the derived values and fingerprint its
/// primary logged: the receiver verifies the fingerprint against its own
/// declaration and stores the logged values without deriving them again.
/// The entry is honored only on the cluster-internal node surface, whose
/// callers authenticate as members; the coordinator builds node requests
/// itself and never forwards a client's metadata.
pub const WAL_REPLAY_METADATA: &str = "x-protomolt-wal-replay-bin";

/// The [`WAL_REPLAY_METADATA`] value for a stream starting at `first_id`.
pub fn encode_replay_first_id(first_id: u64) -> [u8; 8] {
    first_id.to_le_bytes()
}

/// The first id a [`WAL_REPLAY_METADATA`] value names.
pub fn decode_replay_first_id(value: &[u8]) -> Result<u64, String> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        format!(
            "WAL replay metadata holds {} bytes, not an 8-byte first id",
            value.len()
        )
    })?;
    Ok(u64::from_le_bytes(bytes))
}

/// Refuse a stream that needs `what` of a receiver advertising `advertised`.
#[cfg(feature = "net")]
pub(crate) fn require_receiver(advertised: u32, what: &str) -> Result<(), String> {
    if advertised < RECEIVER_VERSION {
        return Err(format!(
            "receiver replication version {advertised} cannot accept {what}; upgrade the receiver"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_replay_first_id_round_trips_and_any_other_width_is_refused() {
        for id in [0, 1, u64::from(u32::MAX) + 7, u64::MAX] {
            assert_eq!(decode_replay_first_id(&encode_replay_first_id(id)), Ok(id));
        }
        for width in [0, 1, 7, 9, 16] {
            let error = decode_replay_first_id(&vec![0; width]).unwrap_err();
            assert!(error.contains(&format!("holds {width} bytes")), "{error}");
        }
    }

    #[cfg(feature = "net")]
    #[test]
    fn an_older_receiver_is_refused_by_name() {
        let error = require_receiver(0, "keyed rows").unwrap_err();
        assert_eq!(
            error,
            "receiver replication version 0 cannot accept keyed rows; upgrade the receiver"
        );
        assert!(require_receiver(RECEIVER_VERSION, "keyed rows").is_ok());
    }
}
