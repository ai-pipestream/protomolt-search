//! Dense execution plan resolution against the live provider fleet.

use super::dense_status;
use super::provider::DenseProviderFacts;
use super::*;
use crate::dense_execution::{DenseRequest, ProviderCaps, RawDenseControls, ResolvedDensePlan};
use crate::dense_policy::{DenseExecutionPolicy, Qualification, Qualified};

impl DenseProviderFacts {
    /// The executable capabilities the fleet proved for a route shape. Staged
    /// selection is unary-only, and settle/deadline finish windows are driven
    /// only on single-leaf streams.
    pub(super) fn caps(&self, streaming: bool, finish_windows: bool) -> ProviderCaps {
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
            staged: self.staged && !streaming,
            patience: self.supports_patience,
            floor_pruning: self.supports_floor_pruning,
            candidate_stream: self.supports_stream && finish_windows,
            rows: self.rows,
        }
    }

    fn live_identity(&self) -> crate::dense_policy::LiveIdentity {
        crate::dense_policy::LiveIdentity {
            provider_backend: self.provider.clone(),
            scoring_fingerprint: self.scoring_fingerprint.clone(),
            corpus_generation: self.generation,
            corpus_rows: self.rows,
            deleted_docs: self.deleted_docs,
            dimensions: self.dimensions,
            corpus_fingerprint: self.corpus_fingerprint.clone(),
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

/// What a policy decided for an AUTO request.
struct PolicyDecision {
    plan: ResolvedDensePlan,
    qualified: Option<Box<Qualified>>,
    selectivity_ppm: u32,
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
        key: DenseRequestKey<'_>,
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
        let (streaming, finish_windows) = self.route_transport(key);
        let caps = facts.caps(streaming, finish_windows);
        let decision = match (&self.dense_execution_policy, request.mode) {
            (Some(policy), crate::pb::DenseExecutionMode::Auto) => {
                self.decide_auto(policy, request, &facts, &caps, key, streaming)
                    .await?
            }
            _ => PolicyDecision {
                plan: crate::dense_execution::resolve(request, &caps).map_err(dense_status)?,
                qualified: None,
                selectivity_ppm: 0,
            },
        };
        let mut outcome = crate::dense_execution::planned_outcome(&decision.plan);
        outcome.provider_backend = facts.provider.clone();
        outcome.quality_contract = facts.quality as i32;
        outcome.scoring_fingerprint = facts.scoring_fingerprint.clone();
        if let Some(hit) = decision.qualified {
            stamp_provenance(&mut outcome, &hit, decision.selectivity_ppm);
        }
        Ok(DensePlanned {
            plan: decision.plan,
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

    /// Validate raw dense controls on a direct `Search` call and check peer
    /// capabilities before dispatching to shards.
    pub(in crate::coordinator) async fn validate_raw_dense_controls(
        &self,
        raw: &RawDenseControls,
    ) -> Result<u64, Status> {
        let streaming = self.stream_search
            || raw.finish_settle_us.is_some()
            || raw.finish_deadline_us.is_some();
        self.validate_raw_controls_for_route(raw, streaming, true)
            .await
    }

    /// Validate raw dense controls on a direct `HybridSearch` call, classifying
    /// transport from the fusion mode (`Decomposed` streams; other modes run
    /// unary leg rounds; none drive finish windows).
    pub(in crate::coordinator) async fn validate_hybrid_raw_dense_controls(
        &self,
        raw: &RawDenseControls,
        decomposed: bool,
    ) -> Result<u64, Status> {
        self.validate_raw_controls_for_route(raw, decomposed, false)
            .await
    }

    async fn validate_raw_controls_for_route(
        &self,
        raw: &RawDenseControls,
        streaming: bool,
        finish_windows: bool,
    ) -> Result<u64, Status> {
        crate::dense_execution::validate_raw_controls(raw, None).map_err(dense_status)?;
        if !raw.has_any() {
            return Ok(0);
        }
        let facts = match self.clustered_quality_identity().await? {
            Some(identity) => DenseProviderFacts::clustered(identity, false),
            None => self.shard_dense_facts().await?,
        };
        let caps = facts.caps(streaming, finish_windows);
        crate::dense_execution::validate_raw_controls(raw, Some(&caps)).map_err(dense_status)?;
        Ok(facts.rows)
    }

    /// Read the live corpus and provider identity a version-2 policy binds to.
    pub async fn live_dense_identity(&self) -> Result<crate::dense_policy::LiveIdentity, Status> {
        let facts = match self.clustered_quality_identity().await? {
            Some(identity) => DenseProviderFacts::clustered(identity, false),
            None => self.shard_dense_facts().await?,
        };
        Ok(facts.live_identity())
    }

    fn route_transport(&self, key: DenseRequestKey<'_>) -> (bool, bool) {
        match key.route_mode {
            DenseRouteMode::SingleLeaf => (self.request_streams(key.filters), true),
            DenseRouteMode::CompositeUnary => (false, false),
            DenseRouteMode::CompositeDecomposed => (true, false),
        }
    }

    fn request_streams(&self, filters: Option<&RequestFilters>) -> bool {
        self.stream_search
            || filters
                .is_some_and(|f| f.finish_settle_us.is_some() || f.finish_deadline_us.is_some())
    }

    async fn decide_auto(
        &self,
        policy: &DenseExecutionPolicy,
        request: DenseRequest,
        facts: &DenseProviderFacts,
        caps: &ProviderCaps,
        key: DenseRequestKey<'_>,
        streaming: bool,
    ) -> Result<PolicyDecision, Status> {
        if let Err(drift) = policy.verify_identity(&facts.live_identity()) {
            return auto_drift_fallback(request, caps, drift, 0);
        }
        let selectivity_ppm = self
            .dense_filter_selectivity(key.filters, facts.rows)
            .await?;
        let shape = if streaming {
            crate::pb::DenseExecutionShape::Streaming
        } else {
            crate::pb::DenseExecutionShape::Unary
        };
        let output_precision =
            crate::dense_execution::output_precision(&request, caps).map_err(dense_status)?;
        let qualification = policy.qualify(crate::dense_policy::RequestKey {
            k: key.k,
            candidate_depth: key.candidate_depth,
            filter_selectivity_ppm: selectivity_ppm,
            output_precision,
            shape,
        });
        match qualification {
            Qualification::Qualified(hit) => {
                match crate::dense_execution::qualified(request, caps, &hit.point) {
                    Ok(plan) => Ok(PolicyDecision {
                        plan,
                        qualified: Some(hit),
                        selectivity_ppm,
                    }),
                    Err(crate::dense_execution::DenseExecutionError::FailedPrecondition(drift)) => {
                        auto_drift_fallback(request, caps, drift, selectivity_ppm)
                    }
                    Err(err) => Err(dense_status(err)),
                }
            }
            Qualification::Unqualified(reason) => {
                let mut plan =
                    crate::dense_execution::resolve(request, caps).map_err(dense_status)?;
                plan.fallback_reason = reason;
                Ok(PolicyDecision {
                    plan,
                    qualified: None,
                    selectivity_ppm,
                })
            }
        }
    }
}

fn auto_drift_fallback(
    request: DenseRequest,
    caps: &ProviderCaps,
    drift: String,
    selectivity_ppm: u32,
) -> Result<PolicyDecision, Status> {
    crate::metrics::record_dense_policy_drift();
    eprintln!("warning: {drift}; AUTO falling back to exhaustive execution");
    let mut plan = crate::dense_execution::resolve(request, caps).map_err(|err| {
        dense_status(
            crate::dense_execution::DenseExecutionError::FailedPrecondition(format!(
                "{drift}; {err}"
            )),
        )
    })?;
    plan.fallback_reason = drift;
    Ok(PolicyDecision {
        plan,
        qualified: None,
        selectivity_ppm,
    })
}

/// The measured evidence behind a qualified AUTO plan.
fn stamp_provenance(
    outcome: &mut crate::pb::DenseExecutionOutcome,
    hit: &Qualified,
    selectivity_ppm: u32,
) {
    outcome.policy_id = hit.policy_id.clone();
    outcome.policy_fingerprint = hit.policy_fingerprint.clone();
    outcome.policy_point = Some(hit.point);
    outcome.filter_selectivity_ppm = selectivity_ppm;
    outcome.candidate_depth = hit.point.candidates;
    outcome.evidence_scope = crate::pb::DenseEvidenceScope::SelectivityBandBenchmark as i32;
}

#[cfg(test)]
mod tests {
    use crate::coordinator::{
        CoordinatorServiceImpl, DenseRequestKey, DenseRouteMode, RequestFilters,
    };

    #[test]
    fn request_streams_tracks_stream_search_and_finish_windows() {
        let unary = CoordinatorServiceImpl::new(Vec::new());
        assert!(!unary.request_streams(None));
        assert!(!unary.request_streams(Some(&RequestFilters::default())));

        let settle = RequestFilters {
            finish_settle_us: Some(250),
            ..Default::default()
        };
        assert!(unary.request_streams(Some(&settle)));

        let deadline = RequestFilters {
            finish_deadline_us: Some(1_000),
            ..Default::default()
        };
        assert!(unary.request_streams(Some(&deadline)));

        let streaming = unary.with_stream_search(true);
        assert!(streaming.request_streams(None));
    }

    #[test]
    fn route_transport_distinguishes_single_leaf_unary_and_decomposed_composites() {
        let unary = CoordinatorServiceImpl::new(Vec::new());
        let streaming = unary.clone().with_stream_search(true);
        let key = |route_mode| DenseRequestKey {
            k: 10,
            candidate_depth: 0,
            route_mode,
            filters: None,
        };
        assert_eq!(
            unary.route_transport(key(DenseRouteMode::SingleLeaf)),
            (false, true)
        );
        assert_eq!(
            streaming.route_transport(key(DenseRouteMode::SingleLeaf)),
            (true, true)
        );
        assert_eq!(
            unary.route_transport(key(DenseRouteMode::CompositeUnary)),
            (false, false)
        );
        assert_eq!(
            streaming.route_transport(key(DenseRouteMode::CompositeUnary)),
            (false, false)
        );
        assert_eq!(
            unary.route_transport(key(DenseRouteMode::CompositeDecomposed)),
            (true, false)
        );
        assert_eq!(
            streaming.route_transport(key(DenseRouteMode::CompositeDecomposed)),
            (true, false)
        );
    }
}
