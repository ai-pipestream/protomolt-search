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
    pub(super) rows: u64,
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
            rows: identity.rows,
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
    rows: u64,
}

impl ShardFactsFold {
    /// Every shard is one provider under one fingerprint, so
    /// one that lacks a regime means none can be relied on.
    fn new() -> Self {
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
            rows: 0,
        }
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

    fn finish(self) -> DenseProviderFacts {
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
            rows: self.rows,
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
        for task in tasks {
            let (addr, backend) = task.await.map_err(|error| {
                Status::internal(format!("dense execution preflight failed: {error}"))
            })??;
            fold.rows = fold
                .rows
                .checked_add(backend.num_vectors)
                .ok_or_else(|| Status::internal("dense execution preflight row count overflow"))?;
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
        Ok(fold.finish())
    }
}
