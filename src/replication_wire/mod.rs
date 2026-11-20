//! The wire contract of replication append streams, shared by the sender
//! (`replication`, which needs the network) and every node receiver,
//! embedded builds included: per-row stable keys and the WAL replay mark.

mod keyed;
mod receiver;

#[cfg(feature = "net")]
pub(crate) use self::keyed::encoded_key_bytes;
pub use self::keyed::{decode_stable_keys, STABLE_KEYS_METADATA};
#[cfg(feature = "net")]
pub use self::keyed::{encode_stable_keys, plan_keyed_streams, MAX_STREAM_KEY_BYTES};
pub use self::receiver::{decode_replay_first_id, WAL_REPLAY_METADATA};
#[cfg(feature = "net")]
pub use self::receiver::encode_replay_first_id;
