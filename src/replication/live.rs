//! The live tail of a split: the source's durable prefix applied to the
//! already-installed child images, routed by stable key.

use prost::Message;
use tonic::transport::Channel;

use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::wal::{wal_record, LoggedAddDocuments, LoggedAddVectors, WalRecord};
use crate::pb::{FlushRequest, HealthRequest, HealthResponse, ReadWalRequest, ReadWalResponse};

use super::{
    client, keyed_request, require_fresh_ingest_replay, vector_row, LiveChild, LiveReshardState,
};

/// The children must tile the whole hash space: no gap, no overlap, no
/// inversion.
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

async fn health_of(addr: &str, what: &str) -> Result<HealthResponse, String> {
    Ok(client(addr)?
        .health(HealthRequest {})
        .await
        .map_err(|error| format!("{what} health {addr}: {error}"))?
        .into_inner())
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
    let source_health = health_of(&source, "source").await?;
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
        let health = health_of(&child.addr, "child").await?;
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

/// One child's connection and what the retry must skip: rows a previous
/// attempt appended past the durable checkpoint.
struct ChildTail {
    client: NodeServiceClient<Channel>,
    skip_vectors: u64,
    skip_documents: u64,
    document_version: u32,
    vector_version: u32,
}

impl ChildTail {
    async fn open(child: &LiveChild) -> Result<Self, String> {
        let mut client = client(&child.addr)?;
        let health = client
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
        Ok(Self {
            client,
            skip_vectors: health.num_vectors - expected_vectors,
            skip_documents: health.document_slots - expected_documents,
            document_version: health.document_contract_version,
            vector_version: health.multi_res_vector_version,
        })
    }
}

/// The source's counts at the end of the exported prefix.
struct LivePrefix {
    vectors: u64,
    documents: u64,
    scoring: String,
    slot_offset: u64,
}

/// A live tail in progress: the state it advances and the children's
/// connections.
struct LiveTail {
    updated: LiveReshardState,
    children: Vec<ChildTail>,
}

/// Apply the next durable source prefix to already-installed child images.
/// The state is retry-safe across the flush/cursor-write crash window by
/// reconciling each child's actual append tips with the checkpointed tips.
pub async fn catch_up_children_once(state: &LiveReshardState) -> Result<LiveReshardState, String> {
    validate_child_ranges(&state.children)?;
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
    let mut children = Vec::with_capacity(state.children.len());
    for child in &state.children {
        children.push(ChildTail::open(child).await?);
    }
    let mut tail = LiveTail {
        updated: state.clone(),
        children,
    };
    let stream = source
        .read_wal(ReadWalRequest {
            generation: state.source_wal_generation,
            after_clock: state.source_clock,
        })
        .await
        .map_err(|error| format!("read live source WAL: {error}"))?
        .into_inner();
    let (high_watermark, prefix) = tail.apply_stream(stream, state.source_clock).await?;
    if prefix.slot_offset != source_health.slot_offset {
        return Err("live WAL prefix slot offset differs from source health".to_string());
    }
    tail.flush_and_verify(&prefix).await?;
    tail.updated.source_clock = high_watermark;
    Ok(tail.updated)
}

impl LiveTail {
    async fn apply_stream(
        &mut self,
        mut stream: tonic::Streaming<ReadWalResponse>,
        mut high_watermark: u64,
    ) -> Result<(u64, LivePrefix), String> {
        let mut prefix = None;
        while let Some(frame) = stream
            .message()
            .await
            .map_err(|error| format!("stream live source WAL: {error}"))?
        {
            high_watermark = frame.high_watermark;
            if frame.completed {
                prefix = Some(LivePrefix {
                    vectors: frame.num_vectors,
                    documents: frame.document_slots,
                    scoring: frame.scoring_fingerprint,
                    slot_offset: frame.slot_offset,
                });
                continue;
            }
            let record = WalRecord::decode(frame.record.as_slice())
                .map_err(|error| format!("decode live source WAL: {error}"))?;
            self.apply(record).await?;
        }
        let reconciled = self
            .children
            .iter()
            .all(|child| child.skip_vectors == 0 && child.skip_documents == 0);
        match prefix {
            Some(prefix) if reconciled => Ok((high_watermark, prefix)),
            _ => Err("live child catch-up ended without a complete, reconciled prefix".to_string()),
        }
    }

    async fn apply(&mut self, record: WalRecord) -> Result<(), String> {
        match record.op {
            Some(wal_record::Op::AddVectors(add)) => self.apply_vectors(add).await,
            Some(wal_record::Op::AddDocuments(add)) => self.apply_documents(add).await,
            Some(wal_record::Op::DeleteDocument(_)) | Some(wal_record::Op::Replacement(_)) => {
                Err("live child catch-up refuses delete/replacement records".to_string())
            }
            Some(wal_record::Op::Snapshot(snapshot)) => Err(format!(
                "source rotated from snapshot generation {}; rebuild the child baseline",
                snapshot.source_generation
            )),
            Some(wal_record::Op::Bind(_)) | Some(wal_record::Op::Flush(_)) | None => Ok(()),
        }
    }

    async fn apply_vectors(&mut self, add: LoggedAddVectors) -> Result<(), String> {
        let batch = add
            .batch
            .ok_or_else(|| "WAL vector record has no batch".to_string())?;
        let dim = batch.dim as usize;
        if dim == 0 || !batch.vectors.len().is_multiple_of(dim) {
            return Err("live WAL vector record has invalid dimensions".to_string());
        }
        let rows = batch.vectors.len() / dim;
        if add.stable_routing_keys.len() != rows {
            return Err("live child catch-up requires one stable key per vector".to_string());
        }
        crate::multi_res_wal::decode_batch(&batch)?;
        for (offset, (vector, key)) in batch
            .vectors
            .chunks_exact(dim)
            .zip(&add.stable_routing_keys)
            .enumerate()
        {
            let index = self.updated.route(key)?;
            let child = &mut self.children[index];
            crate::multi_res_wal::require_supported(child.vector_version, &batch)?;
            if child.skip_vectors > 0 {
                child.skip_vectors -= 1;
            } else {
                let request = keyed_request(vector_row(&batch, offset, vector), key);
                child
                    .client
                    .add_vectors(request)
                    .await
                    .map_err(|error| format!("append child vector: {error}"))?;
            }
            self.updated.children[index].applied_vectors += 1;
        }
        Ok(())
    }

    async fn apply_documents(&mut self, add: LoggedAddDocuments) -> Result<(), String> {
        if add.stable_routing_keys.len() != add.documents.len() {
            return Err("live child catch-up requires one stable key per document".to_string());
        }
        for (document, key) in add.documents.into_iter().zip(&add.stable_routing_keys) {
            let index = self.updated.route(key)?;
            let child = &mut self.children[index];
            let required = crate::document_contract::required_version(&document);
            crate::document_contract::require_supported(child.document_version, required)?;
            require_fresh_ingest_replay(&document)?;
            if child.skip_documents > 0 {
                child.skip_documents -= 1;
            } else {
                let response = child
                    .client
                    .add_documents(keyed_request(document, key))
                    .await
                    .map_err(|error| format!("append child document: {error}"))?
                    .into_inner();
                crate::document_contract::require_supported(
                    response.document_contract_version,
                    required,
                )?;
            }
            self.updated.children[index].applied_documents += 1;
        }
        Ok(())
    }

    /// Flush every child, then hold their summed counts and each scoring
    /// fingerprint to the source's prefix.
    async fn flush_and_verify(&mut self, prefix: &LivePrefix) -> Result<(), String> {
        for child in &mut self.children {
            child
                .client
                .flush(FlushRequest {})
                .await
                .map_err(|error| format!("flush live child: {error}"))?;
        }
        let (mut vectors, mut documents) = (0u64, 0u64);
        for (child, tail) in self.updated.children.iter().zip(&mut self.children) {
            let health = tail
                .client
                .health(HealthRequest {})
                .await
                .map_err(|error| format!("verify live child {}: {error}", child.addr))?
                .into_inner();
            vectors += health.num_vectors;
            documents += health.document_slots;
            if health.scoring_fingerprint != prefix.scoring {
                return Err(format!(
                    "child {} scoring fingerprint differs from source",
                    child.addr
                ));
            }
        }
        if vectors != prefix.vectors || documents != prefix.documents {
            return Err(format!(
                "live child verification count mismatch: source {}/{} vectors/documents, children {vectors}/{documents}",
                prefix.vectors, prefix.documents
            ));
        }
        Ok(())
    }
}
