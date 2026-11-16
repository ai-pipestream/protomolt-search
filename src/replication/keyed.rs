//! Per-row stable routing keys for one append stream: the metadata value a
//! keyed stream of several rows carries, and how a keyed record's rows are
//! cut into such streams.
//!
//! The value holds one key per row of the stream, in row order, each as a
//! little-endian `u32` length followed by the key bytes. A receiver takes
//! the keys positionally and must refuse a stream whose row count differs
//! from the key count, so a row can never be appended under a neighbor's
//! key or under none.

use std::ops::Range;

/// Binary metadata entry naming one stable routing key per row of an
/// append stream.
pub const STABLE_KEYS_METADATA: &str = "x-protomolt-stable-keys-bin";

/// Encoded key bytes one stream's metadata value carries at most. Binary
/// metadata travels base64-encoded inside the HTTP/2 header block, so the
/// cap leaves a third of headroom under a 16 KiB header list.
pub const MAX_STREAM_KEY_BYTES: usize = 8 << 10;

const LENGTH_BYTES: usize = std::mem::size_of::<u32>();

/// The metadata value for `keys`, one per row in row order.
pub fn encode_stable_keys(keys: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let mut value = Vec::with_capacity(keys.iter().map(|key| LENGTH_BYTES + key.len()).sum());
    for key in keys {
        let length = u32::try_from(key.len())
            .map_err(|_| "stable routing key exceeds the 4 GiB frame limit".to_string())?;
        value.extend_from_slice(&length.to_le_bytes());
        value.extend_from_slice(key);
    }
    Ok(value)
}

/// The keys of a metadata value, one per row in row order. A value that
/// ends inside a length or a key is refused whole.
pub fn decode_stable_keys(mut value: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut keys = Vec::new();
    while !value.is_empty() {
        let (length, rest) = value
            .split_first_chunk::<LENGTH_BYTES>()
            .ok_or_else(|| "stable-keys metadata ends inside a key length".to_string())?;
        let length = u32::from_le_bytes(*length) as usize;
        if rest.len() < length {
            return Err("stable-keys metadata ends inside a key".to_string());
        }
        let (key, rest) = rest.split_at(length);
        keys.push(key.to_vec());
        value = rest;
    }
    Ok(keys)
}

/// Cut the rows of a keyed record into contiguous streams of at most
/// `max_rows` rows whose encoded keys fit [`MAX_STREAM_KEY_BYTES`]. Every
/// row lands in exactly one stream, in order; a row whose key alone
/// exceeds the cap travels in a stream of its own.
pub fn plan_keyed_streams(keys: &[Vec<u8>], max_rows: usize) -> Vec<Range<usize>> {
    let max_rows = max_rows.max(1);
    let mut streams = Vec::new();
    let (mut start, mut bytes) = (0, 0);
    for (row, key) in keys.iter().enumerate() {
        let encoded = LENGTH_BYTES + key.len();
        if row > start && (row - start == max_rows || bytes + encoded > MAX_STREAM_KEY_BYTES) {
            streams.push(start..row);
            (start, bytes) = (row, 0);
        }
        bytes += encoded;
    }
    if start < keys.len() {
        streams.push(start..keys.len());
    }
    streams
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(lengths: &[usize]) -> Vec<Vec<u8>> {
        lengths
            .iter()
            .enumerate()
            .map(|(row, &length)| vec![row as u8; length])
            .collect()
    }

    #[test]
    fn keys_round_trip_in_row_order_including_empty_and_repeated_ones() {
        let rows = vec![b"a".to_vec(), Vec::new(), b"a".to_vec(), vec![0xFF; 300]];
        let value = encode_stable_keys(&rows).unwrap();
        assert_eq!(value.len(), 4 * LENGTH_BYTES + 302);
        assert_eq!(decode_stable_keys(&value).unwrap(), rows);
        assert_eq!(encode_stable_keys(&[]).unwrap(), Vec::<u8>::new());
        assert_eq!(decode_stable_keys(&[]).unwrap(), Vec::<Vec<u8>>::new());
    }

    #[test]
    fn a_truncated_value_is_refused_whole() {
        let value = encode_stable_keys(&keys(&[3, 5])).unwrap();
        for cut in 1..value.len() {
            let decoded = decode_stable_keys(&value[..cut]);
            if cut == LENGTH_BYTES + 3 {
                assert_eq!(decoded.unwrap(), keys(&[3]));
            } else {
                assert!(decoded.is_err(), "cut at {cut}");
            }
        }
        let mut long = value.clone();
        long.push(0);
        assert!(decode_stable_keys(&long).is_err());
    }

    #[test]
    fn streams_cover_every_row_once_in_order_within_both_caps() {
        let cases = [
            keys(&[]),
            keys(&[1]),
            keys(&[10; 7]),
            keys(&[4_000, 4_000, 4_000, 10]),
            keys(&[MAX_STREAM_KEY_BYTES * 2, 1, 1]),
        ];
        for rows in &cases {
            for max_rows in [0, 1, 3, 512] {
                let streams = plan_keyed_streams(rows, max_rows);
                let covered: Vec<usize> = streams.iter().flat_map(Clone::clone).collect();
                assert_eq!(covered, (0..rows.len()).collect::<Vec<_>>());
                for stream in streams {
                    assert!(stream.len() <= max_rows.max(1));
                    let value = encode_stable_keys(&rows[stream.clone()]).unwrap();
                    assert!(value.len() <= MAX_STREAM_KEY_BYTES || stream.len() == 1);
                }
            }
        }
        assert_eq!(plan_keyed_streams(&keys(&[10; 7]), 3), [0..3, 3..6, 6..7]);
        let wide = keys(&[4_000, 4_000, 4_000, 10]);
        assert_eq!(plan_keyed_streams(&wide, 512), [0..2, 2..4]);
    }
}
