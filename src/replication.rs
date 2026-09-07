//! Durable single-writer WAL catch-up for one primary/replica pair.
//!
//! The primary exports only a flushed generation-clock prefix. Applying that
//! prefix is idempotent by positional tip: a crash after target flush but
//! before cursor persistence causes the retry to skip rows already present,
//! while gaps and partial rows fail loudly. Deletes and replacements already
//! have idempotent node contracts.

use std::path::{Path, PathBuf};

use prost::Message;
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::search_service_client::SearchServiceClient;
use crate::pb::wal::{wal_record, WalRecord};
use crate::pb::{
    ApplyWalBindingRequest, CommitReplacementsRequest, DeleteDocumentsRequest, FlushRequest,
    HealthRequest, ReadWalRequest,
};

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

    fn route(&self, key: &[u8]) -> Result<usize, String> {
        let hash = crate::coordinator::stable_routing_hash(key);
        self.children
            .iter()
            .position(|child| hash >= child.hash_lo && hash <= child.hash_hi)
            .ok_or_else(|| format!("stable hash {hash} is outside every child range"))
    }
}

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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaCursor {
    pub primary: String,
    pub replica: String,
    pub wal_generation: u64,
    pub clock: u64,
}

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
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create replica state dir {}: {error}", parent.display()))?;
        let tmp = PathBuf::from(format!(
            "{}.tmp.{}.{}",
            path.display(),
            std::process::id(),
            SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let bytes =
            toml::to_string(self).map_err(|error| format!("encode replica state: {error}"))?;
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)
                .map_err(|error| format!("create {}: {error}", tmp.display()))?;
            file.write_all(bytes.as_bytes())
                .and_then(|()| file.sync_all())
                .map_err(|error| format!("write {}: {error}", tmp.display()))?;
        }
        std::fs::rename(&tmp, path)
            .map_err(|error| format!("publish {}: {error}", path.display()))?;
        crate::postings::fsync_parent(path)
            .map_err(|error| format!("fsync replica state {}: {error}", path.display()))
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

/// Bring one replica to the primary's current durable WAL high watermark.
pub async fn sync_once(cursor: &ReplicaCursor) -> Result<ReplicaCursor, String> {
    let mut primary = client(&cursor.primary)?;
    let mut replica = client(&cursor.replica)?;
    let source = primary
        .health(HealthRequest {})
        .await
        .map_err(|error| format!("primary health {}: {error}", cursor.primary))?
        .into_inner();
    if !source.wal_clocked {
        return Err(format!(
            "primary {} has no fully clocked WAL generation",
            cursor.primary
        ));
    }
    let generation = source.wal_generation;
    if cursor.clock != 0 && cursor.wal_generation != generation {
        return Err(format!(
            "primary WAL rotated from generation {} to {generation}; install the new base snapshot before resuming replica catch-up",
            cursor.wal_generation
        ));
    }
    let mut target = replica
        .health(HealthRequest {})
        .await
        .map_err(|error| format!("replica health {}: {error}", cursor.replica))?
        .into_inner();
    if source.slot_offset != target.slot_offset {
        return Err(format!(
            "replica slot offset {} does not match primary {}",
            target.slot_offset, source.slot_offset
        ));
    }
    let mut stream = primary
        .read_wal(ReadWalRequest {
            generation,
            after_clock: if cursor.wal_generation == generation {
                cursor.clock
            } else {
                0
            },
        })
        .await
        .map_err(|error| format!("read primary WAL: {error}"))?
        .into_inner();
    let mut high_watermark = cursor.clock;
    let mut completed = false;
    let mut prefix_health = None;
    let mut requires_flush = false;
    while let Some(frame) = stream
        .message()
        .await
        .map_err(|error| format!("stream primary WAL: {error}"))?
    {
        if frame.generation != generation {
            return Err("primary changed WAL generation inside one catch-up stream".to_string());
        }
        high_watermark = frame.high_watermark;
        if frame.completed {
            if !frame.record.is_empty() {
                return Err("WAL completion frame carries record bytes".to_string());
            }
            completed = true;
            prefix_health = Some((
                frame.num_vectors,
                frame.document_slots,
                frame.live_docs,
                frame.deleted_docs,
                frame.scoring_fingerprint,
                frame.slot_offset,
            ));
            continue;
        }
        if completed {
            return Err("WAL record arrived after completion".to_string());
        }
        let record = WalRecord::decode(frame.record.as_slice())
            .map_err(|error| format!("decode WAL record: {error}"))?;
        // An earlier attempt may have applied this record but failed before
        // flushing or acknowledging it. In-memory tips and already_bound do
        // not prove durability, so retries must cross the flush boundary too.
        requires_flush |= matches!(
            record.op.as_ref(),
            Some(
                wal_record::Op::AddVectors(_)
                    | wal_record::Op::AddDocuments(_)
                    | wal_record::Op::DeleteDocument(_)
                    | wal_record::Op::Replacement(_)
                    | wal_record::Op::Bind(_)
            )
        );
        match record.op {
            Some(wal_record::Op::AddVectors(add)) => {
                let batch = add
                    .batch
                    .ok_or_else(|| "WAL vector record has no batch".to_string())?;
                let dim = usize::try_from(batch.dim).unwrap_or(0);
                if dim == 0 || !batch.vectors.len().is_multiple_of(dim) {
                    return Err("WAL vector record has invalid dimensions".to_string());
                }
                let rows = (batch.vectors.len() / dim) as u64;
                if rows == 0 {
                    return Err("WAL vector record is empty".to_string());
                }
                if !add.stable_routing_keys.is_empty()
                    && add.stable_routing_keys.len() != rows as usize
                {
                    return Err("WAL vector stable-key count does not match rows".to_string());
                }
                let tip = target.slot_offset + target.num_vectors;
                if add.first_id < tip {
                    if add.first_id + rows > tip {
                        return Err("replica contains a partial WAL vector batch".to_string());
                    }
                } else if add.first_id == tip {
                    if add.stable_routing_keys.is_empty() {
                        let response = replica
                            .add_vectors(tonic::Request::new(tokio_stream::iter([batch])))
                            .await
                            .map_err(|error| format!("replicate vectors: {error}"))?
                            .into_inner();
                        if response.first_id != add.first_id || response.added != rows {
                            return Err("replica assigned different vector ids".to_string());
                        }
                    } else {
                        for (offset, (vector, key)) in batch
                            .vectors
                            .chunks_exact(dim)
                            .zip(add.stable_routing_keys)
                            .enumerate()
                        {
                            let row = crate::pb::AddVectorsRequest {
                                vectors: vector.to_vec(),
                                dim: batch.dim,
                            };
                            let mut request = tonic::Request::new(tokio_stream::iter([row]));
                            request.metadata_mut().insert_bin(
                                "x-protomolt-stable-key-bin",
                                tonic::metadata::MetadataValue::from_bytes(&key),
                            );
                            let response = replica
                                .add_vectors(request)
                                .await
                                .map_err(|error| format!("replicate vectors: {error}"))?
                                .into_inner();
                            if response.first_id != add.first_id + offset as u64
                                || response.added != 1
                            {
                                return Err("replica assigned different vector ids".to_string());
                            }
                        }
                    }
                    target.num_vectors += rows;
                } else {
                    return Err(format!(
                        "replica vector gap: next id is {tip}, WAL starts at {}",
                        add.first_id
                    ));
                }
            }
            Some(wal_record::Op::AddDocuments(add)) => {
                let required = add
                    .documents
                    .iter()
                    .map(crate::document_contract::required_version)
                    .max()
                    .unwrap_or(0);
                crate::document_contract::require_supported(
                    target.document_contract_version,
                    required,
                )?;
                let rows = add.documents.len() as u64;
                if rows == 0 {
                    return Err("WAL document record is empty".to_string());
                }
                if !add.stable_routing_keys.is_empty()
                    && add.stable_routing_keys.len() != rows as usize
                {
                    return Err("WAL document stable-key count does not match rows".to_string());
                }
                let tip = target.slot_offset + target.document_slots;
                if add.first_id < tip {
                    if add.first_id + rows > tip {
                        return Err("replica contains a partial WAL document batch".to_string());
                    }
                } else if add.first_id == tip {
                    if add.stable_routing_keys.is_empty() {
                        let response = replica
                            .add_documents(tonic::Request::new(tokio_stream::iter(add.documents)))
                            .await
                            .map_err(|error| format!("replicate documents: {error}"))?
                            .into_inner();
                        crate::document_contract::require_supported(
                            response.document_contract_version,
                            required,
                        )?;
                        if response.first_id != add.first_id || response.added != rows {
                            return Err("replica assigned different document ids".to_string());
                        }
                    } else {
                        for (offset, (document, key)) in add
                            .documents
                            .into_iter()
                            .zip(add.stable_routing_keys)
                            .enumerate()
                        {
                            let mut request = tonic::Request::new(tokio_stream::iter([document]));
                            request.metadata_mut().insert_bin(
                                "x-protomolt-stable-key-bin",
                                tonic::metadata::MetadataValue::from_bytes(&key),
                            );
                            let response = replica
                                .add_documents(request)
                                .await
                                .map_err(|error| format!("replicate documents: {error}"))?
                                .into_inner();
                            crate::document_contract::require_supported(
                                response.document_contract_version,
                                required,
                            )?;
                            if response.first_id != add.first_id + offset as u64
                                || response.added != 1
                            {
                                return Err("replica assigned different document ids".to_string());
                            }
                        }
                    }
                    target.document_slots += rows;
                } else {
                    return Err(format!(
                        "replica document gap: next id is {tip}, WAL starts at {}",
                        add.first_id
                    ));
                }
            }
            Some(wal_record::Op::DeleteDocument(delete)) => {
                replica
                    .delete_documents(DeleteDocumentsRequest {
                        expected_wal_generation: None,
                        doc_ids: vec![delete.doc_id],
                    })
                    .await
                    .map_err(|error| format!("replicate delete: {error}"))?;
            }
            Some(wal_record::Op::Replacement(replacement)) => {
                replica
                    .commit_replacements(CommitReplacementsRequest {
                        expected_wal_generation: None,
                        replacements: vec![crate::pb::Replacement {
                            old_doc_id: replacement.old_doc_id,
                            new_doc_id: replacement.new_doc_id,
                        }],
                    })
                    .await
                    .map_err(|error| format!("replicate replacement: {error}"))?;
            }
            Some(wal_record::Op::Snapshot(snapshot)) => {
                return Err(format!(
                    "source WAL contains snapshot marker from generation {}; install its base image before catch-up",
                    snapshot.source_generation
                ));
            }
            Some(wal_record::Op::Bind(binding)) => {
                let analysis_sha = binding.analysis_sha.clone();
                let vector_binding = binding.vector_binding.clone();
                let index_contract = binding.index_contract.clone();
                let response = replica
                    .apply_wal_binding(ApplyWalBindingRequest {
                        collection: String::new(),
                        plan_fingerprint: binding.plan_fingerprint,
                        body_path: binding.body_path,
                        materialize_sha: binding.materialize_sha,
                        analysis_sha: binding.analysis_sha,
                        analysis_contract: binding.analysis_contract,
                        vector_binding: binding.vector_binding,
                        index_contract: binding.index_contract,
                    })
                    .await
                    .map_err(|error| format!("replicate mapped binding: {error}"))?
                    .into_inner();
                if response.analysis_sha != analysis_sha {
                    return Err("replica did not acknowledge the mapped analysis contract; upgrade the receiver".into());
                }
                if response.vector_binding != vector_binding {
                    return Err("replica did not acknowledge the mapped vector binding; upgrade the receiver".into());
                }
                if response.index_contract != index_contract {
                    return Err("replica did not acknowledge the explicit index contract; upgrade the receiver".into());
                }
            }
            Some(wal_record::Op::Flush(_)) | None => {}
        }
    }
    if !completed {
        return Err("primary WAL stream ended without completion".to_string());
    }
    let (
        expected_vectors,
        expected_documents,
        expected_live,
        expected_deleted,
        expected_scoring,
        expected_offset,
    ) = prefix_health.ok_or_else(|| "WAL completion omitted prefix health".to_string())?;
    if expected_offset != source.slot_offset {
        return Err("WAL prefix slot offset differs from primary health".to_string());
    }
    if requires_flush {
        let flushed = replica
            .flush(FlushRequest {})
            .await
            .map_err(|error| format!("flush replica: {error}"))?
            .into_inner();
        if !flushed.written {
            return Err("replica did not persist the WAL prefix (Flush.written=false)".into());
        }
    }
    let verified = replica
        .health(HealthRequest {})
        .await
        .map_err(|error| format!("verify replica: {error}"))?
        .into_inner();
    for (name, expected, actual) in [
        ("vectors", expected_vectors, verified.num_vectors),
        ("documents", expected_documents, verified.document_slots),
        ("live rows", expected_live, verified.live_docs),
        ("deleted rows", expected_deleted, verified.deleted_docs),
    ] {
        if expected != actual {
            return Err(format!(
                "replica verification failed for {name}: primary {expected}, replica {actual}"
            ));
        }
    }
    if expected_scoring != verified.scoring_fingerprint {
        return Err("replica scoring fingerprint differs from primary".to_string());
    }
    Ok(ReplicaCursor {
        primary: cursor.primary.clone(),
        replica: cursor.replica.clone(),
        wal_generation: generation,
        clock: high_watermark,
    })
}

fn validate_child_ranges(children: &[LiveChild]) -> Result<(), String> {
    if children.is_empty() {
        return Err("live reshard requires at least one child".to_string());
    }
    let mut ranges: Vec<(u64, u64)> = children
        .iter()
        .map(|child| (child.hash_lo, child.hash_hi))
        .collect();
    ranges.sort_by_key(|range| range.0);
    let mut expected = 0u64;
    for (index, (lo, hi)) in ranges.iter().copied().enumerate() {
        if lo != expected || lo > hi {
            return Err(format!(
                "live child ranges have a gap, overlap, or inversion at {lo}..={hi}; expected {expected}"
            ));
        }
        if index + 1 == ranges.len() {
            if hi != u64::MAX {
                return Err(format!("live child ranges end at {hi}, not {}", u64::MAX));
            }
        } else {
            expected = hi
                .checked_add(1)
                .ok_or_else(|| "a child reaches the hash-space end too early".to_string())?;
        }
    }
    Ok(())
}

/// Capture the installed child-image tips before any live tail is applied.
pub async fn initialize_live_reshard(
    source: String,
    source_cutoff: crate::reshard::WalCutoff,
    old_topology_generation: u64,
    new_topology_generation: u64,
    mut children: Vec<LiveChild>,
) -> Result<LiveReshardState, String> {
    if new_topology_generation <= old_topology_generation {
        return Err("new topology generation must be strictly newer".to_string());
    }
    validate_child_ranges(&children)?;
    let source_health = client(&source)?
        .health(HealthRequest {})
        .await
        .map_err(|error| format!("source health {source}: {error}"))?
        .into_inner();
    if source_health.wal_generation != source_cutoff.generation
        || source_health.wal_high_watermark < source_cutoff.high_watermark
    {
        return Err(format!(
            "source WAL moved outside the baseline cutoff: built from generation {} clock {}, live generation {} clock {}",
            source_cutoff.generation,
            source_cutoff.high_watermark,
            source_health.wal_generation,
            source_health.wal_high_watermark
        ));
    }
    for child in &mut children {
        let health = client(&child.addr)?
            .health(HealthRequest {})
            .await
            .map_err(|error| format!("child health {}: {error}", child.addr))?
            .into_inner();
        if health.slot_offset != child.slot_offset {
            return Err(format!(
                "child {} slot offset is {}, expected {}",
                child.addr, health.slot_offset, child.slot_offset
            ));
        }
        child.base_vectors = health.num_vectors;
        child.base_document_slots = health.document_slots;
        child.applied_vectors = 0;
        child.applied_documents = 0;
    }
    Ok(LiveReshardState {
        source,
        source_wal_generation: source_cutoff.generation,
        source_clock: source_cutoff.high_watermark,
        old_topology_generation,
        new_topology_generation,
        children,
    })
}

async fn add_vector_with_key(
    client: &mut NodeServiceClient<Channel>,
    batch: crate::pb::AddVectorsRequest,
    key: &[u8],
) -> Result<(), String> {
    let mut request = tonic::Request::new(tokio_stream::iter([batch]));
    request.metadata_mut().insert_bin(
        "x-protomolt-stable-key-bin",
        tonic::metadata::MetadataValue::from_bytes(key),
    );
    client
        .add_vectors(request)
        .await
        .map_err(|error| format!("append child vector: {error}"))?;
    Ok(())
}

async fn add_document_with_key(
    client: &mut NodeServiceClient<Channel>,
    document: crate::pb::AddDocumentsRequest,
    key: &[u8],
) -> Result<(), String> {
    let mut request = tonic::Request::new(tokio_stream::iter([document]));
    request.metadata_mut().insert_bin(
        "x-protomolt-stable-key-bin",
        tonic::metadata::MetadataValue::from_bytes(key),
    );
    client
        .add_documents(request)
        .await
        .map_err(|error| format!("append child document: {error}"))?;
    Ok(())
}

/// Apply the next durable source prefix to already-installed child images.
/// The state is retry-safe across the flush/cursor-write crash window by
/// reconciling each child's actual append tips with the checkpointed tips.
pub async fn catch_up_children_once(state: &LiveReshardState) -> Result<LiveReshardState, String> {
    validate_child_ranges(&state.children)?;
    let mut updated = state.clone();
    let mut source = client(&state.source)?;
    let source_health = source
        .health(HealthRequest {})
        .await
        .map_err(|error| format!("source health {}: {error}", state.source))?
        .into_inner();
    if source_health.wal_generation != state.source_wal_generation {
        return Err(format!(
            "source WAL rotated from generation {} to {}; install a new baseline",
            state.source_wal_generation, source_health.wal_generation
        ));
    }
    if source_health.deleted_docs != 0 {
        return Err(
            "live child catch-up currently requires the append-only product contract; source has deletes"
                .to_string(),
        );
    }
    let mut clients = Vec::with_capacity(state.children.len());
    let mut skip_vectors = Vec::with_capacity(state.children.len());
    let mut skip_documents = Vec::with_capacity(state.children.len());
    for child in &state.children {
        let mut child_client = client(&child.addr)?;
        let health = child_client
            .health(HealthRequest {})
            .await
            .map_err(|error| format!("child health {}: {error}", child.addr))?
            .into_inner();
        let expected_vectors = child.base_vectors + child.applied_vectors;
        let expected_documents = child.base_document_slots + child.applied_documents;
        if health.num_vectors < expected_vectors || health.document_slots < expected_documents {
            return Err(format!(
                "child {} is behind its durable live-reshard checkpoint",
                child.addr
            ));
        }
        skip_vectors.push(health.num_vectors - expected_vectors);
        skip_documents.push(health.document_slots - expected_documents);
        clients.push(child_client);
    }
    let mut stream = source
        .read_wal(ReadWalRequest {
            generation: state.source_wal_generation,
            after_clock: state.source_clock,
        })
        .await
        .map_err(|error| format!("read live source WAL: {error}"))?
        .into_inner();
    let mut high_watermark = state.source_clock;
    let mut completed = false;
    let mut prefix_health = None;
    while let Some(frame) = stream
        .message()
        .await
        .map_err(|error| format!("stream live source WAL: {error}"))?
    {
        high_watermark = frame.high_watermark;
        if frame.completed {
            completed = true;
            prefix_health = Some((
                frame.num_vectors,
                frame.document_slots,
                frame.scoring_fingerprint,
                frame.slot_offset,
            ));
            continue;
        }
        let record = WalRecord::decode(frame.record.as_slice())
            .map_err(|error| format!("decode live source WAL: {error}"))?;
        match record.op {
            Some(wal_record::Op::AddVectors(add)) => {
                let batch = add
                    .batch
                    .ok_or_else(|| "WAL vector record has no batch".to_string())?;
                let dim = batch.dim as usize;
                if dim == 0 || !batch.vectors.len().is_multiple_of(dim) {
                    return Err("live WAL vector record has invalid dimensions".to_string());
                }
                let rows = batch.vectors.len() / dim;
                if add.stable_routing_keys.len() != rows {
                    return Err(
                        "live child catch-up requires one stable key per vector".to_string()
                    );
                }
                for (vector, key) in batch.vectors.chunks_exact(dim).zip(add.stable_routing_keys) {
                    let child = updated.route(&key)?;
                    if skip_vectors[child] > 0 {
                        skip_vectors[child] -= 1;
                    } else {
                        add_vector_with_key(
                            &mut clients[child],
                            crate::pb::AddVectorsRequest {
                                vectors: vector.to_vec(),
                                dim: dim as u32,
                            },
                            &key,
                        )
                        .await?;
                    }
                    updated.children[child].applied_vectors += 1;
                }
            }
            Some(wal_record::Op::AddDocuments(add)) => {
                if add.stable_routing_keys.len() != add.documents.len() {
                    return Err(
                        "live child catch-up requires one stable key per document".to_string()
                    );
                }
                for (document, key) in add.documents.into_iter().zip(add.stable_routing_keys) {
                    let child = updated.route(&key)?;
                    if skip_documents[child] > 0 {
                        skip_documents[child] -= 1;
                    } else {
                        add_document_with_key(&mut clients[child], document, &key).await?;
                    }
                    updated.children[child].applied_documents += 1;
                }
            }
            Some(wal_record::Op::DeleteDocument(_)) | Some(wal_record::Op::Replacement(_)) => {
                return Err("live child catch-up refuses delete/replacement records".to_string())
            }
            Some(wal_record::Op::Snapshot(snapshot)) => {
                return Err(format!(
                    "source rotated from snapshot generation {}; rebuild the child baseline",
                    snapshot.source_generation
                ))
            }
            Some(wal_record::Op::Bind(_)) | Some(wal_record::Op::Flush(_)) | None => {}
        }
    }
    if !completed
        || skip_vectors.iter().any(|count| *count != 0)
        || skip_documents.iter().any(|count| *count != 0)
    {
        return Err("live child catch-up ended without a complete, reconciled prefix".to_string());
    }
    let (expected_vectors, expected_documents, expected_scoring, expected_offset) =
        prefix_health.ok_or_else(|| "live WAL completion omitted prefix health".to_string())?;
    if expected_offset != source_health.slot_offset {
        return Err("live WAL prefix slot offset differs from source health".to_string());
    }
    for client in &mut clients {
        client
            .flush(FlushRequest {})
            .await
            .map_err(|error| format!("flush live child: {error}"))?;
    }
    let mut vectors = 0u64;
    let mut documents = 0u64;
    for (child, client) in updated.children.iter().zip(&mut clients) {
        let health = client
            .health(HealthRequest {})
            .await
            .map_err(|error| format!("verify live child {}: {error}", child.addr))?
            .into_inner();
        vectors += health.num_vectors;
        documents += health.document_slots;
        if health.scoring_fingerprint != expected_scoring {
            return Err(format!(
                "child {} scoring fingerprint differs from source",
                child.addr
            ));
        }
    }
    if vectors != expected_vectors || documents != expected_documents {
        return Err(format!(
            "live child verification count mismatch: source {}/{} vectors/documents, children {vectors}/{documents}",
            expected_vectors, expected_documents
        ));
    }
    updated.source_clock = high_watermark;
    Ok(updated)
}

/// Freeze old-generation writes, catch up and verify the final WAL prefix,
/// durably publish the map file, atomically swap the live coordinator map,
/// and release the barrier. Queries continue on their frozen old snapshots.
pub async fn atomic_live_cutover(
    coordinator: &str,
    state: &LiveReshardState,
    state_path: &Path,
    shard_map_path: &Path,
) -> Result<LiveReshardState, String> {
    let endpoint =
        tonic::transport::Endpoint::from_shared(crate::security::process_secure_url(coordinator))
            .map_err(|error| format!("invalid coordinator address: {error}"))?;
    let endpoint = crate::security::secure_endpoint(endpoint)?;
    let mut control = SearchServiceClient::new(endpoint.connect_lazy())
        .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);
    let frozen = control
        .freeze_topology_writes(crate::pb::FreezeTopologyWritesRequest {
            collection: String::new(),
            required_topology_generation: state.old_topology_generation,
        })
        .await
        .map_err(|error| format!("freeze topology writes: {error}"))?
        .into_inner();
    let token = frozen.cutover_token;
    let mut durable_map_published = false;
    let result = async {
        let caught_up = catch_up_children_once(state).await?;
        caught_up.write(state_path)?;
        let map = crate::config::ShardMap {
            generation: caught_up.new_topology_generation,
            shards: caught_up
                .children
                .iter()
                .map(|child| crate::config::ShardMapShard {
                    addr: child.addr.clone(),
                    replica: child.replica.clone(),
                    slot_offset: child.slot_offset,
                    hash_lo: Some(child.hash_lo),
                    hash_hi: Some(child.hash_hi),
                    placement: None,
                })
                .collect(),
            placement: None,
        };
        write_toml_atomic(shard_map_path, &map, "shard map")?;
        durable_map_published = true;
        let published = control
            .publish_topology(crate::pb::PublishTopologyRequest {
                collection: String::new(),
                cutover_token: token,
                generation: map.generation,
                shards: map
                    .shards
                    .iter()
                    .map(|shard| crate::pb::PublishedTopologyShard {
                        addr: shard.addr.clone(),
                        replica: shard.replica.clone().unwrap_or_default(),
                        hash_lo: shard.hash_lo.unwrap_or_default(),
                        hash_hi: shard.hash_hi.unwrap_or_default(),
                        has_placement: shard.placement.is_some(),
                        placement: shard.placement.unwrap_or_default(),
                    })
                    .collect(),
                placement: map.placement.as_ref().map(|tree| tree.to_proto()),
            })
            .await
            .map_err(|error| format!("publish topology: {error}"))?
            .into_inner();
        if published.topology_generation != map.generation {
            return Err("coordinator echoed the wrong topology generation".to_string());
        }
        Ok(caught_up)
    }
    .await;
    if result.is_err() && !durable_map_published {
        let _ = control
            .abort_topology_cutover(crate::pb::AbortTopologyCutoverRequest {
                collection: String::new(),
                cutover_token: token,
            })
            .await;
    }
    match result {
        Err(error) if durable_map_published => Err(format!(
            "{error}; the new shard map is durable at {} and the coordinator remains write-frozen. Retry publication or restart the coordinator from that map before accepting writes",
            shard_map_path.display()
        )),
        other => other,
    }
}
