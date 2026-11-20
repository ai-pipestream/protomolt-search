//! Durable single-writer WAL catch-up for one primary/replica pair, and
//! the live tail of a source shard into the children of a split.
//!
//! The primary exports only a flushed generation-clock prefix. Applying that
//! prefix is idempotent by positional tip: a crash after target flush but
//! before cursor persistence causes the retry to skip rows already present,
//! while gaps and partial rows fail loudly. Deletes and replacements already
//! have idempotent node contracts.
//!
//! Consecutive append records share one stream (`batch`); a record is
//! admitted against the tips and checked for one stable key per row before
//! it is held. Keyed rows travel several to a stream under the per-row key
//! value `replication_wire` defines, and document streams are marked as WAL
//! replay so logged derived values are verified rather than derived
//! again. The live reshard tail routes each keyed row to its child alone.

mod append;
mod batch;
mod cutover;
mod live;
mod sync;

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use crate::pb::node_service_client::NodeServiceClient;

pub use self::cutover::atomic_live_cutover;
pub use self::live::{catch_up_children_once, initialize_live_reshard};
pub use self::sync::sync_once;
pub use crate::replication_wire::{
    decode_replay_first_id, decode_stable_keys, encode_replay_first_id, encode_stable_keys,
    plan_keyed_streams, MAX_STREAM_KEY_BYTES, STABLE_KEYS_METADATA, WAL_REPLAY_METADATA,
};

/// One child of a live reshard: where it serves, the hash range and
/// slots it takes, its tips when the baseline was installed, and the
/// rows the tail has applied to it since.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveChild {
    pub addr: String,
    pub replica: Option<String>,
    pub hash_lo: u64,
    pub hash_hi: u64,
    pub slot_offset: u64,
    pub base_vectors: u64,
    pub base_document_slots: u64,
    pub applied_vectors: u64,
    pub applied_documents: u64,
}

/// The durable cursor of a live reshard: the source log position the
/// children hold through, and the topology generations it moves between.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveReshardState {
    pub source: String,
    pub source_wal_generation: u64,
    pub source_clock: u64,
    pub old_topology_generation: u64,
    pub new_topology_generation: u64,
    pub children: Vec<LiveChild>,
}

impl LiveReshardState {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("read live-reshard state {}: {error}", path.display()))?;
        toml::from_str(&text)
            .map_err(|error| format!("parse live-reshard state {}: {error}", path.display()))
    }

    pub fn write(&self, path: &Path) -> Result<(), String> {
        write_toml_atomic(path, self, "live-reshard state")
    }

    /// The child whose range holds the stable key's hash.
    fn route(&self, key: &[u8]) -> Result<usize, String> {
        let hash = crate::coordinator::stable_routing_hash(key);
        self.children
            .iter()
            .position(|child| hash >= child.hash_lo && hash <= child.hash_hi)
            .ok_or_else(|| format!("stable hash {hash} is outside every child range"))
    }
}

/// Write `value` as TOML beside `path`, sync it, rename it into place,
/// and sync the directory. `what` names the record in failures.
fn write_toml_atomic<T: Serialize>(path: &Path, value: &T, what: &str) -> Result<(), String> {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("create {what} dir {}: {error}", parent.display()))?;
    let tmp = PathBuf::from(format!(
        "{}.tmp.{}.{}",
        path.display(),
        std::process::id(),
        SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let bytes = toml::to_string(value).map_err(|error| format!("encode {what}: {error}"))?;
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)
            .map_err(|error| format!("create {}: {error}", tmp.display()))?;
        file.write_all(bytes.as_bytes())
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("write {}: {error}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).map_err(|error| format!("publish {}: {error}", path.display()))?;
    crate::postings::fsync_parent(path)
        .map_err(|error| format!("fsync {what} {}: {error}", path.display()))
}

/// How far one replica has applied one primary's log.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaCursor {
    pub primary: String,
    pub replica: String,
    pub wal_generation: u64,
    pub clock: u64,
}

/// Every replica cursor a process keeps.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicaState {
    #[serde(default)]
    pub cursors: Vec<ReplicaCursor>,
}

impl ReplicaState {
    pub fn load(path: &Path) -> Result<Self, String> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("read replica state {}: {error}", path.display()))?;
        toml::from_str(&text)
            .map_err(|error| format!("parse replica state {}: {error}", path.display()))
    }

    pub fn cursor_mut(&mut self, primary: &str, replica: &str) -> &mut ReplicaCursor {
        if let Some(index) = self
            .cursors
            .iter()
            .position(|cursor| cursor.primary == primary && cursor.replica == replica)
        {
            return &mut self.cursors[index];
        }
        self.cursors.push(ReplicaCursor {
            primary: primary.to_string(),
            replica: replica.to_string(),
            ..Default::default()
        });
        self.cursors.last_mut().expect("cursor was just inserted")
    }

    pub fn write(&self, path: &Path) -> Result<(), String> {
        write_toml_atomic(path, self, "replica state")
    }
}

fn client(addr: &str) -> Result<NodeServiceClient<Channel>, String> {
    let endpoint =
        tonic::transport::Endpoint::from_shared(crate::security::process_secure_url(addr))
            .map_err(|error| format!("invalid node address {addr}: {error}"))?
            .tcp_nodelay(true)
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW);
    let endpoint = crate::security::secure_endpoint(endpoint)?;
    Ok(NodeServiceClient::new(endpoint.connect_lazy())
        .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(crate::MAX_MESSAGE_BYTES))
}

/// A one-row append stream carrying the row's stable routing key.
fn keyed_request<T>(
    row: T,
    key: &[u8],
) -> Result<tonic::Request<tokio_stream::Iter<std::array::IntoIter<T, 1>>>, String> {
    let mut request = tonic::Request::new(tokio_stream::iter([row]));
    let encoded = encode_stable_keys(std::slice::from_ref(&key.to_vec()))?;
    request.metadata_mut().insert_bin(
        STABLE_KEYS_METADATA,
        tonic::metadata::MetadataValue::from_bytes(&encoded),
    );
    Ok(request)
}

/// The live reshard tail sends each row to a child as fresh ingest, so a
/// logged derived row has no contract there: carrying its values would need
/// the child's tip in a WAL replay stream, which the tail does not track
/// per row. The check runs before the row is sent, and a refusal ends the
/// tail without advancing its cursor.
fn require_fresh_ingest_replay(document: &crate::pb::AddDocumentsRequest) -> Result<(), String> {
    if !document.derived_fingerprint.is_empty() {
        return Err(
            "the live reshard tail sends fresh ingest and cannot carry logged derived values"
                .to_string(),
        );
    }
    Ok(())
}

/// Vector row `offset` of a logged batch, with its retained payload.
fn vector_row(
    batch: &crate::pb::AddVectorsRequest,
    offset: usize,
    vector: &[f32],
) -> crate::pb::AddVectorsRequest {
    crate::pb::AddVectorsRequest {
        materialized_rows: batch
            .materialized_rows
            .get(offset)
            .cloned()
            .into_iter()
            .collect(),
        vectors: vector.to_vec(),
        dim: batch.dim,
    }
}
