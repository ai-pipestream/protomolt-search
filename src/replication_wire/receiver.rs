//! Metadata of replication append streams: per-row stable keys and the
//! WAL replay mark.

/// Binary metadata entry marking a document stream as WAL replay. The
/// value is the little-endian `u64` global id of the stream's first row,
/// which must be the receiver's document tip when the stream opens.
///
/// A replayed document keeps the derived values and fingerprint its
/// primary logged: the receiver verifies the fingerprint against its own
/// declaration and stores the logged values without deriving them again.
/// Cluster members use the trusted node channel; a bearer-authenticated
/// caller needs an ADMIN grant on the receiver's collection.
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
}
