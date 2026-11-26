//! Fleet-wide dense provider facts gathered before a dense route runs.

use super::*;

/// What one dense preflight proved about the live provider.
pub(super) struct DenseProviderFacts {
    pub(super) provider: String,
    pub(super) scoring_fingerprint: String,
    pub(super) quality: crate::pb::VectorQualityContract,
    pub(super) exhaustive: bool,
    pub(super) uncertified_scan: bool,
    pub(super) fp32_sidecar: bool,
    pub(super) enumerates: bool,
    pub(super) supports_patience: bool,
    pub(super) supports_floor_pruning: bool,
    pub(super) supports_stream: bool,
    pub(super) dimensions: u32,
    /// 32 for providers that score original FP32 rows.
    pub(super) bits_per_dimension: u32,
    /// Every shard can select by staged top-k over owned bit planes.
    pub(super) staged: bool,
    pub(super) rows: u64,
    pub(super) deleted_docs: u64,
    pub(super) generation: u64,
    pub(super) corpus_fingerprint: String,
}

impl DenseProviderFacts {
    /// The clustered backend runs quantized collection searches remotely
    /// without returning per-part scan counts; shard-local FP32 sidecar
    /// scanning and rescoring remain available when product shards own
    /// original FP32 rows.
    pub(super) fn clustered(
        identity: crate::clustered_turbovec::ClusteredQualityIdentity,
        fp32_sidecar: bool,
    ) -> Self {
        let mut corpus_hash = turbo_sha256::Sha256::new();
        corpus_hash.update(b"dense-corpus-clustered-v1");
        corpus_hash.update(&identity.rows.to_le_bytes());
        corpus_hash.update(&identity.topology_generation.to_le_bytes());
        corpus_hash.update(&(identity.scoring_fingerprint.len() as u64).to_le_bytes());
        corpus_hash.update(identity.scoring_fingerprint.as_bytes());
        Self {
            provider: "clustered-turbovec".to_string(),
            scoring_fingerprint: identity.scoring_fingerprint,
            quality: crate::pb::VectorQualityContract::ExhaustiveNativeScore,
            exhaustive: false,
            uncertified_scan: true,
            fp32_sidecar,
            enumerates: false,
            supports_patience: false,
            supports_floor_pruning: false,
            supports_stream: true,
            dimensions: identity.dimensions,
            bits_per_dimension: 0,
            staged: false,
            rows: identity.rows,
            deleted_docs: 0,
            generation: identity.topology_generation,
            corpus_fingerprint: corpus_hash.finish().to_hex(),
        }
    }

    /// Whether the provider proves exhaustive native scoring and completion.
    pub(super) fn exact_available(&self) -> bool {
        self.quality == crate::pb::VectorQualityContract::ExhaustiveNativeScore && self.exhaustive
    }

    /// Whether the provider exposes a configured approximate traversal.
    pub(super) fn configured_ann(&self) -> bool {
        matches!(
            self.quality,
            crate::pb::VectorQualityContract::ConfiguredAnn
                | crate::pb::VectorQualityContract::ProbabilisticBound
        )
    }
}

/// Per-shard descriptors folded into one fleet-wide agreement.
struct ShardFactsFold {
    provider: Option<String>,
    fingerprint: Option<String>,
    quality: Option<crate::pb::VectorQualityContract>,
    exhaustive: Option<bool>,
    enumerates: bool,
    supports_patience: bool,
    supports_floor_pruning: bool,
    supports_stream: bool,
    dimensions: Option<u32>,
    bits_per_dimension: Option<u32>,
    staged: bool,
    rows: u64,
    deleted_docs: u64,
    corpus_hash: turbo_sha256::Sha256,
}

impl ShardFactsFold {
    /// Every shard is one provider under one fingerprint, so
    /// one that lacks a regime means none can be relied on.
    fn new() -> Self {
        let mut corpus_hash = turbo_sha256::Sha256::new();
        corpus_hash.update(b"dense-corpus-state-v1");
        Self {
            provider: None,
            fingerprint: None,
            quality: None,
            exhaustive: None,
            enumerates: true,
            supports_patience: true,
            supports_floor_pruning: true,
            supports_stream: true,
            dimensions: None,
            bits_per_dimension: None,
            staged: true,
            rows: 0,
            deleted_docs: 0,
            corpus_hash,
        }
    }

    fn absorb_state(
        &mut self,
        shard: usize,
        addr: &str,
        backend: &crate::pb::GetVectorBackendResponse,
    ) -> Result<(), Status> {
        if backend.stats_incarnation.is_empty() {
            return Err(Status::failed_precondition(format!(
                "dense execution preflight: shard {shard} ({addr}) reported an empty \
                 stats_incarnation"
            )));
        }
        self.corpus_hash.update(&(shard as u64).to_le_bytes());
        self.corpus_hash.update(&backend.num_vectors.to_le_bytes());
        self.corpus_hash.update(&backend.deleted_docs.to_le_bytes());
        self.corpus_hash
            .update(&backend.wal_generation.to_le_bytes());
        self.corpus_hash
            .update(&backend.wal_high_watermark.to_le_bytes());
        self.corpus_hash.update(&backend.stats_epoch.to_le_bytes());
        self.corpus_hash
            .update(&(backend.stats_incarnation.len() as u64).to_le_bytes());
        self.corpus_hash.update(&backend.stats_incarnation);
        Ok(())
    }

    fn absorb(
        &mut self,
        descriptor: crate::pb::VectorBackendDescriptor,
        shard_quality: crate::pb::VectorQualityContract,
    ) -> Result<(), Status> {
        let shard_exhaustive = shard_quality
            == crate::pb::VectorQualityContract::ExhaustiveNativeScore
            && advertises(&descriptor, "exhaustive_completion");
        self.enumerates &= advertises(&descriptor, "mask_enumeration");
        self.staged &= advertises(&descriptor, "staged_top_k");
        self.supports_patience &= advertises(&descriptor, "patience_early_stop");
        self.supports_floor_pruning &= advertises(&descriptor, "floor_pruning");
        self.supports_stream &= advertises(&descriptor, "candidate_stream");

        hold_uniform(
            &mut self.provider,
            descriptor.backend_kind,
            "dense execution preflight found mixed vector providers",
        )?;
        hold_uniform(
            &mut self.fingerprint,
            descriptor.scoring_fingerprint,
            "dense execution preflight found mixed scoring fingerprints",
        )?;
        hold_uniform(
            &mut self.quality,
            shard_quality,
            "dense execution preflight found mixed quality contracts",
        )?;
        hold_uniform(
            &mut self.exhaustive,
            shard_exhaustive,
            "dense execution preflight found mixed completion capabilities",
        )?;
        hold_uniform(
            &mut self.bits_per_dimension,
            descriptor.bits_per_dimension,
            "dense execution preflight found mixed score precisions",
        )?;
        hold_uniform(
            &mut self.dimensions,
            descriptor.dim,
            "dense execution preflight found mixed vector dimensions",
        )
    }

    fn finish(self, generation: u64) -> DenseProviderFacts {
        let configured = self.provider.is_some();
        let exhaustive = self.exhaustive.unwrap_or(false);
        let quality = self
            .quality
            .unwrap_or(crate::pb::VectorQualityContract::Unspecified);
        DenseProviderFacts {
            provider: self.provider.unwrap_or_default(),
            scoring_fingerprint: self.fingerprint.unwrap_or_default(),
            quality,
            exhaustive,
            uncertified_scan: false,
            fp32_sidecar: quality == crate::pb::VectorQualityContract::ExhaustiveNativeScore
                && exhaustive,
            enumerates: self.enumerates && configured,
            supports_patience: self.supports_patience && configured,
            supports_floor_pruning: self.supports_floor_pruning && configured,
            supports_stream: self.supports_stream && configured,
            dimensions: self.dimensions.unwrap_or_default(),
            bits_per_dimension: self.bits_per_dimension.unwrap_or_default(),
            staged: self.staged && configured,
            rows: self.rows,
            deleted_docs: self.deleted_docs,
            generation,
            corpus_fingerprint: self.corpus_hash.finish().to_hex(),
        }
    }
}

fn advertises(descriptor: &crate::pb::VectorBackendDescriptor, capability: &str) -> bool {
    descriptor
        .capabilities
        .iter()
        .any(|advertised| advertised == capability)
}

/// Refuse a shard descriptor the coordinator cannot merge, and return the
/// quality contract it declares.
fn shard_quality_contract(
    descriptor: &crate::pb::VectorBackendDescriptor,
) -> Result<crate::pb::VectorQualityContract, Status> {
    if descriptor.backend_kind.is_empty() {
        return Err(Status::failed_precondition(
            "dense execution preflight found an unnamed vector provider",
        ));
    }
    if descriptor.scoring_fingerprint.is_empty() {
        return Err(Status::failed_precondition(
            "dense execution preflight found an empty scoring fingerprint",
        ));
    }
    let shard_quality = crate::pb::VectorQualityContract::try_from(descriptor.quality_contract)
        .map_err(|_| {
            Status::failed_precondition(format!(
                "vector provider {} advertises unknown quality contract {}",
                descriptor.backend_kind, descriptor.quality_contract
            ))
        })?;
    if shard_quality == crate::pb::VectorQualityContract::Unspecified {
        return Err(Status::failed_precondition(format!(
            "vector provider {} does not declare a quality contract",
            descriptor.backend_kind
        )));
    }
    let direction =
        crate::pb::VectorScoreDirection::try_from(descriptor.score_direction).map_err(|_| {
            Status::failed_precondition(format!(
                "vector provider {} advertises unknown score direction {}",
                descriptor.backend_kind, descriptor.score_direction
            ))
        })?;
    if direction != crate::pb::VectorScoreDirection::HigherIsBetter {
        return Err(Status::failed_precondition(format!(
            "vector provider {} is not compatible with the coordinator's higher-is-better heap",
            descriptor.backend_kind
        )));
    }
    if !advertises(descriptor, "batch_query") {
        return Err(Status::failed_precondition(format!(
            "vector provider {} does not advertise batch_query",
            descriptor.backend_kind
        )));
    }
    Ok(shard_quality)
}

impl CoordinatorServiceImpl {
    /// Read every shard's vector backend and fold them into one provider
    /// identity, refusing any disagreement between shards.
    pub(super) async fn shard_dense_facts(&self) -> Result<DenseProviderFacts, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for addr in &self.node_addrs {
            let mut client = self.node_client(addr)?;
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                client
                    .get_vector_backend(crate::pb::GetVectorBackendRequest {})
                    .await
                    .map(|resp| (addr, resp.into_inner()))
            }));
        }

        let mut fold = ShardFactsFold::new();
        for (shard_idx, task) in tasks.into_iter().enumerate() {
            let (addr, backend) = task.await.map_err(|error| {
                Status::internal(format!("dense execution preflight failed: {error}"))
            })??;
            fold.rows = fold
                .rows
                .checked_add(backend.num_vectors)
                .ok_or_else(|| Status::internal("dense execution preflight row count overflow"))?;
            fold.deleted_docs = fold
                .deleted_docs
                .checked_add(backend.deleted_docs)
                .ok_or_else(|| {
                    Status::internal("dense execution preflight deleted_docs overflow")
                })?;
            fold.absorb_state(shard_idx, &addr, &backend)?;
            let descriptor = backend.descriptor.ok_or_else(|| {
                Status::failed_precondition(
                    "dense execution preflight found an unconfigured vector shard",
                )
            })?;
            self.shard_vector_rows
                .lock()
                .expect("shard vector row cache poisoned")
                .insert(addr, backend.num_vectors);
            let shard_quality = shard_quality_contract(&descriptor)?;
            fold.absorb(descriptor, shard_quality)?;
        }
        Ok(fold.finish(self.topology_generation))
    }
}

#[cfg(test)]
mod tests {
    use super::ShardFactsFold;
    use crate::pb::GetVectorBackendResponse;

    #[test]
    fn absorb_state_refuses_empty_stats_incarnation() {
        let mut fold = ShardFactsFold::new();
        let empty = GetVectorBackendResponse::default();
        let err = fold
            .absorb_state(0, "http://127.0.0.1:50051", &empty)
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("stats_incarnation"), "{err}");

        let populated = GetVectorBackendResponse {
            stats_incarnation: vec![7; 32],
            ..Default::default()
        };
        fold.absorb_state(0, "http://127.0.0.1:50051", &populated)
            .unwrap();
    }
}
