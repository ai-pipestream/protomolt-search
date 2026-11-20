//! The wire contract of replication append streams, shared by the sender
//! (`replication`, which needs the network) and every node receiver,
//! embedded builds included: per-row stable keys and the WAL replay mark.

mod keyed;
mod receiver;

#[cfg(feature = "net")]
pub(crate) use self::keyed::encoded_key_bytes;
pub use self::keyed::{
    decode_stable_keys, encode_stable_keys, plan_keyed_streams, MAX_STREAM_KEY_BYTES,
    STABLE_KEYS_METADATA,
};
pub use self::receiver::{decode_replay_first_id, encode_replay_first_id, WAL_REPLAY_METADATA};
