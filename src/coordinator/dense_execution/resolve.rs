//! Dense execution mode resolution against the live provider.

use super::provider::DenseProviderFacts;
use super::*;

/// The traversal a request resolved to, with the planner's reason and the
/// policy point that qualified it when AUTO went through a policy.
struct DenseModePlan {
    resolved: crate::pb::DenseExecutionMode,
    planner_reason: String,
    qualified: Option<(crate::dense_policy::Qualified, u32)>,
}

impl DenseModePlan {
    fn unqualified(resolved: crate::pb::DenseExecutionMode, planner_reason: &str) -> Self {
        Self {
            resolved,
            planner_reason: planner_reason.to_string(),
            qualified: None,
        }
    }
}

impl CoordinatorServiceImpl {
    /// Prove the dense traversal contract against every live shard before the
    /// public query route runs. Provider identity is generation-wide: mixed
    /// quality contracts, score spaces, or dimensions are refused rather than
    /// hidden behind one coordinator response.
    pub(crate) async fn resolve_dense_execution(
        &self,
        requested: crate::pb::DenseExecutionMode,
        query_dim: usize,
        key: DenseRequestKey<'_>,
    ) -> Result<crate::pb::DenseExecutionOutcome, Status> {
        let facts = match self.clustered_quality_identity().await? {
            Some(identity) => DenseProviderFacts::clustered(identity),
            None => self.shard_dense_facts().await?,
        };
        if facts.dimensions as usize != query_dim {
            return Err(Status::failed_precondition(format!(
                "dense query dimension {query_dim} does not match live provider dimension {}",
                facts.dimensions
            )));
        }
        let plan = self.plan_dense_mode(requested, &facts, key).await?;
        Ok(dense_outcome(requested, facts, plan))
    }

    async fn plan_dense_mode(
        &self,
        requested: crate::pb::DenseExecutionMode,
        facts: &DenseProviderFacts,
        key: DenseRequestKey<'_>,
    ) -> Result<DenseModePlan, Status> {
        let exact_available = facts.exact_available();
        match requested {
            crate::pb::DenseExecutionMode::Unspecified => plan_unspecified(facts),
            crate::pb::DenseExecutionMode::Exact => plan_exact(facts),
            crate::pb::DenseExecutionMode::Ann => plan_ann(facts),
            crate::pb::DenseExecutionMode::Auto if exact_available => {
                Ok(DenseModePlan::unqualified(
                    crate::pb::DenseExecutionMode::Exact,
                    "AUTO selected exact because the live provider proves exhaustive completion",
                ))
            }
            // AUTO on a provider without exhaustive completion goes only
            // through the generation-bound policy: identity first, then
            // the exact request key. Nothing is interpolated or defaulted.
            crate::pb::DenseExecutionMode::Auto => {
                let (hit, selectivity, planner_reason) = self.qualify_auto(facts, key).await?;
                Ok(DenseModePlan {
                    resolved: crate::pb::DenseExecutionMode::Ann,
                    planner_reason,
                    qualified: Some((hit, selectivity)),
                })
            }
        }
    }
}

fn plan_unspecified(facts: &DenseProviderFacts) -> Result<DenseModePlan, Status> {
    if !facts.exact_available() {
        return Err(Status::failed_precondition(format!(
            "unspecified dense execution preserves the exact contract, but provider {} advertises {:?} without exhaustive completion; request ANN explicitly or install an exact backend",
            facts.provider, facts.quality
        )));
    }
    Ok(DenseModePlan::unqualified(
        crate::pb::DenseExecutionMode::Exact,
        "legacy unspecified mode preserves exact traversal",
    ))
}

fn plan_exact(facts: &DenseProviderFacts) -> Result<DenseModePlan, Status> {
    if !facts.exact_available() {
        return Err(Status::failed_precondition(format!(
            "EXACT dense execution requires exhaustive native scoring and completion, but provider {} advertises {:?}",
            facts.provider, facts.quality
        )));
    }
    Ok(DenseModePlan::unqualified(
        crate::pb::DenseExecutionMode::Exact,
        "caller required exact traversal",
    ))
}

fn plan_ann(facts: &DenseProviderFacts) -> Result<DenseModePlan, Status> {
    if facts.exact_available() {
        return Err(Status::failed_precondition(format!(
            "ANN dense execution was requested, but provider {} exposes only exhaustive traversal",
            facts.provider
        )));
    }
    if !facts.configured_ann() {
        return Err(Status::failed_precondition(format!(
            "provider {} does not expose a configured ANN traversal",
            facts.provider
        )));
    }
    Ok(DenseModePlan::unqualified(
        crate::pb::DenseExecutionMode::Ann,
        "caller accepted the provider's configured approximate traversal",
    ))
}

fn dense_outcome(
    requested: crate::pb::DenseExecutionMode,
    facts: DenseProviderFacts,
    plan: DenseModePlan,
) -> crate::pb::DenseExecutionOutcome {
    let mut outcome = crate::pb::DenseExecutionOutcome {
        requested_mode: requested as i32,
        resolved_mode: plan.resolved as i32,
        provider_backend: facts.provider,
        quality_contract: facts.quality as i32,
        scoring_fingerprint: facts.scoring_fingerprint,
        exhaustive_completion: plan.resolved == crate::pb::DenseExecutionMode::Exact,
        planner_reason: plan.planner_reason,
        policy_id: String::new(),
        policy_fingerprint: String::new(),
        policy_point: None,
        filter_selectivity_ppm: 0,
        candidate_depth: 0,
        evidence_scope: crate::pb::DenseEvidenceScope::NotApplicable as i32,
        filter_regimes: None,
        tail_enumerated_rows: 0,
        patience_stopped: 0,
        shards_finished_early: 0,
        shards_floor_pruned: 0,
    };
    if let Some((hit, selectivity)) = plan.qualified {
        outcome.evidence_scope = crate::pb::DenseEvidenceScope::SelectivityBandBenchmark as i32;
        outcome.policy_id = hit.policy_id;
        outcome.policy_fingerprint = hit.policy_fingerprint;
        outcome.policy_point = Some(crate::pb::DensePolicyPoint {
            k: hit.point.k,
            filter_selectivity_ppm_min: hit.point.selectivity_min_ppm,
            filter_selectivity_ppm_max: hit.point.selectivity_max_ppm,
            candidates: hit.point.candidates,
            measured_recall_ppm: hit.point.measured_recall_ppm,
            enumerate_max_rows: hit.point.enumerate_max_rows,
            patience_saturation_ppm: hit.point.patience_saturation_ppm,
            patience_hops: hit.point.patience_hops,
            finish_settle_us: hit.point.finish_settle_us,
            floor_greediness_ppm: hit.point.floor_greediness_ppm,
            floor_clamp_min: hit.point.floor_clamp_min,
        });
        outcome.filter_selectivity_ppm = selectivity;
        outcome.candidate_depth = hit.point.candidates;
    }
    outcome
}
