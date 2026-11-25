//! Dense execution plan resolution against the live provider fleet.

use super::dense_status;
use super::provider::DenseProviderFacts;
use super::*;
use crate::dense_execution::{DenseRequest, ProviderCaps, RawDenseControls, ResolvedDensePlan};

impl DenseProviderFacts {
    /// The executable capabilities the fleet proved.
    pub(super) fn caps(&self) -> ProviderCaps {
        ProviderCaps {
            provider: self.provider.clone(),
            native: if self.bits_per_dimension == 32 {
                crate::pb::DenseScorePrecision::Fp32
            } else {
                crate::pb::DenseScorePrecision::Quantized
            },
            exhaustive_scan: self.exact_available(),
            uncertified_scan: self.uncertified_scan,
            fp32_sidecar: self.fp32_sidecar,
            walks: self.configured_ann(),
            enumerates: self.enumerates,
            patience: self.supports_patience,
            floor_pruning: self.supports_floor_pruning,
            candidate_stream: self.supports_stream,
            rows: self.rows,
        }
    }
}

/// A plan together with the outcome the route will complete.
pub(crate) struct DensePlanned {
    pub(crate) plan: ResolvedDensePlan,
    pub(crate) outcome: crate::pb::DenseExecutionOutcome,
    /// Physical rows across the fleet.
    pub(crate) rows: u64,
}

impl CoordinatorServiceImpl {
    /// Prove the dense execution contract against every live shard before a
    /// dense route runs. Provider identity is generation-wide: mixed
    /// providers, score spaces, precisions or dimensions are refused rather
    /// than hidden behind one coordinator response.
    pub(crate) async fn resolve_dense_execution(
        &self,
        request: DenseRequest,
        query_dim: usize,
    ) -> Result<DensePlanned, Status> {
        let facts = match self.clustered_quality_identity().await? {
            Some(identity) => {
                let fp32_sidecar = if request.precision == crate::pb::DensePrecisionRequest::Fp32 {
                    self.clustered_fp32_sidecar_available(identity.rows).await?
                } else {
                    false
                };
                DenseProviderFacts::clustered(identity, fp32_sidecar)
            }
            None => self.shard_dense_facts().await?,
        };
        if facts.dimensions as usize != query_dim {
            return Err(Status::failed_precondition(format!(
                "dense query dimension {query_dim} does not match live provider dimension {}",
                facts.dimensions
            )));
        }
        let plan = crate::dense_execution::resolve(request, &facts.caps()).map_err(dense_status)?;
        let mut outcome = crate::dense_execution::planned_outcome(&plan);
        outcome.provider_backend = facts.provider.clone();
        outcome.quality_contract = facts.quality as i32;
        outcome.scoring_fingerprint = facts.scoring_fingerprint.clone();
        Ok(DensePlanned {
            plan,
            outcome,
            rows: facts.rows,
        })
    }

    async fn clustered_fp32_sidecar_available(&self, clustered_rows: u64) -> Result<bool, Status> {
        if self.node_addrs.is_empty() {
            return Ok(false);
        }
        if clustered_rows == 0 {
            return Ok(true);
        }
        let mut exact_rows = 0u64;
        for addr in &self.node_addrs {
            let mut client = self.node_client(addr)?;
            let health = client.health(HealthRequest {}).await?.into_inner();
            exact_rows = exact_rows.saturating_add(health.exact_vector_rows);
        }
        if exact_rows == 0 {
            return Ok(false);
        }
        // The certificate covers the product shards' rows; it certifies the
        // clustered corpus only when the two hold the same row count.
        if exact_rows != clustered_rows {
            return Err(Status::failed_precondition(format!(
                "FP32 sidecar holds {exact_rows} rows across the product shards but the \
                 clustered backend serves {clustered_rows}; an exhaustive FP32 scan of one \
                 cannot certify the other"
            )));
        }
        Ok(true)
    }

    /// Validate raw dense controls on direct `Search` / `HybridSearch` calls
    /// and check peer capabilities before dispatching to shards.
    pub(in crate::coordinator) async fn validate_raw_dense_controls(
        &self,
        raw: &RawDenseControls,
    ) -> Result<u64, Status> {
        crate::dense_execution::validate_raw_controls(raw, None).map_err(dense_status)?;
        if !raw.has_any() {
            return Ok(0);
        }
        let facts = match self.clustered_quality_identity().await? {
            Some(identity) => DenseProviderFacts::clustered(identity, false),
            None => self.shard_dense_facts().await?,
        };
        crate::dense_execution::validate_raw_controls(raw, Some(&facts.caps()))
            .map_err(dense_status)?;
        Ok(facts.rows)
    }
}
