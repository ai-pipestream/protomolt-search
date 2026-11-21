//! The replication metadata of one append stream, as the receiver reads
//! it: the stable routing key of each row, and whether the stream replays
//! a primary's WAL.

use tonic::metadata::MetadataMap;
use tonic::Status;

use crate::replication_wire::{
    decode_replay_first_id, decode_stable_keys, STABLE_KEYS_METADATA, WAL_REPLAY_METADATA,
};

/// The stable routing keys of one append stream, handed out one per row
/// in arrival order. A keyed stream must carry exactly as many rows as it
/// names keys: a row past the last key is refused before it applies, and
/// a stream that ends with keys left over is refused when it closes.
pub(in crate::node) struct StreamKeys {
    keys: std::vec::IntoIter<Vec<u8>>,
    keyed: bool,
}

impl StreamKeys {
    /// Decode the per-row stable keys named by `metadata`.
    pub(in crate::node) fn from_metadata(metadata: &MetadataMap) -> Result<Self, Status> {
        let keys = metadata_bytes(metadata, STABLE_KEYS_METADATA)?
            .map(|value| decode_stable_keys(&value).map_err(Status::invalid_argument))
            .transpose()?;
        if keys.as_ref().is_some_and(Vec::is_empty) {
            return Err(Status::invalid_argument(
                "replication stable-keys metadata names no keys",
            ));
        }
        Ok(Self {
            keyed: keys.is_some(),
            keys: keys.unwrap_or_default().into_iter(),
        })
    }

    /// The key of the stream's next row; `None` for an unkeyed stream or
    /// a row whose entry is empty (an unkeyed row inside a keyed stream:
    /// an empty key routes nothing and proves no colocation).
    pub(in crate::node) fn next_row(&mut self, what: &str) -> Result<Option<Vec<u8>>, Status> {
        if !self.keyed {
            return Ok(None);
        }
        self.keys
            .next()
            .map(|key| (!key.is_empty()).then_some(key))
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "the replication stream carries more {what} rows than stable keys"
                ))
            })
    }

    /// Refuse a keyed stream that closed before every key had its row.
    pub(in crate::node) fn finish(&self, what: &str) -> Result<(), Status> {
        match self.keys.len() {
            0 => Ok(()),
            left => Err(Status::invalid_argument(format!(
                "the replication stream ended with {left} stable keys and no {what} rows for them"
            ))),
        }
    }
}

/// The first id a WAL replay stream claims, when `metadata` marks one.
pub(in crate::node) fn replay_first_id(metadata: &MetadataMap) -> Result<Option<u64>, Status> {
    metadata_bytes(metadata, WAL_REPLAY_METADATA)?
        .map(|value| decode_replay_first_id(&value).map_err(Status::invalid_argument))
        .transpose()
}

fn metadata_bytes(metadata: &MetadataMap, name: &str) -> Result<Option<Vec<u8>>, Status> {
    metadata
        .get_bin(name)
        .map(|value| {
            value
                .to_bytes()
                .map(|bytes| bytes.to_vec())
                .map_err(|error| {
                    Status::invalid_argument(format!("invalid {name} metadata: {error}"))
                })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication_wire::{encode_replay_first_id, encode_stable_keys};
    use tonic::metadata::MetadataValue;

    fn metadata(entries: &[(&'static str, &[u8])]) -> MetadataMap {
        let mut map = MetadataMap::new();
        for (name, value) in entries {
            map.insert_bin(*name, MetadataValue::from_bytes(value));
        }
        map
    }

    #[test]
    fn keys_are_handed_out_one_per_row_and_must_all_be_used() {
        let value = encode_stable_keys(&[b"a".to_vec(), Vec::new(), b"c".to_vec()]).unwrap();
        let mut keys =
            StreamKeys::from_metadata(&metadata(&[(STABLE_KEYS_METADATA, &value)])).unwrap();
        assert_eq!(keys.next_row("document").unwrap(), Some(b"a".to_vec()));
        let left = keys.finish("document").unwrap_err();
        assert!(
            left.message().contains("ended with 2 stable keys"),
            "{left}"
        );
        assert_eq!(keys.next_row("document").unwrap(), None);
        assert_eq!(keys.next_row("document").unwrap(), Some(b"c".to_vec()));
        keys.finish("document").unwrap();
        let extra = keys.next_row("document").unwrap_err();
        assert!(extra
            .message()
            .contains("more document rows than stable keys"));
    }

    #[test]
    fn no_keys_name_an_unkeyed_stream() {
        let mut none = StreamKeys::from_metadata(&MetadataMap::new()).unwrap();
        for _ in 0..3 {
            assert_eq!(none.next_row("vector").unwrap(), None);
        }
        none.finish("vector").unwrap();
    }

    #[test]
    fn an_empty_list_or_a_truncated_value_is_refused() {
        let value = encode_stable_keys(&[b"a".to_vec()]).unwrap();
        let empty = metadata(&[(STABLE_KEYS_METADATA, &[])]);
        let error = StreamKeys::from_metadata(&empty).err().unwrap();
        assert!(error.message().contains("names no keys"), "{error}");
        let cut = metadata(&[(STABLE_KEYS_METADATA, &value[..value.len() - 1])]);
        assert!(StreamKeys::from_metadata(&cut).is_err());
    }

    #[test]
    fn a_replay_mark_names_its_first_id() {
        assert_eq!(replay_first_id(&MetadataMap::new()).unwrap(), None);
        let marked = metadata(&[(WAL_REPLAY_METADATA, &encode_replay_first_id(41))]);
        assert_eq!(replay_first_id(&marked).unwrap(), Some(41));
        let short = metadata(&[(WAL_REPLAY_METADATA, &[1, 2, 3])]);
        assert!(replay_first_id(&short).is_err());
    }
}
