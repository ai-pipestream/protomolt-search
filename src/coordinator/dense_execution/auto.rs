//! AUTO qualification through the generation-bound dense execution policy.

use super::provider::DenseProviderFacts;
use super::*;

impl CoordinatorServiceImpl {
    /// Qualify AUTO on a provider without exhaustive completion: the policy
    /// identity must match the live provider, and the request key must hit
    /// one measured point whose regimes the provider can serve. Returns the
    /// hit, the live filter selectivity, and the planner reason.
    pub(super) async fn qualify_auto(
        &self,
        facts: &DenseProviderFacts,
        key: DenseRequestKey<'_>,
    ) -> Result<(crate::dense_policy::Qualified, u32, String), Status> {
        let provider = &facts.provider;
        let policy = self.dense_execution_policy.as_ref().ok_or_else(|| {
            Status::failed_precondition(format!(
                "AUTO has no dense execution policy for provider {provider} ({:?}); \
                 install one with --dense-execution-policy, or use ANN explicitly to \
                 accept its configured traversal",
                facts.quality
            ))
        })?;
        if !facts.configured_ann() {
            return Err(Status::failed_precondition(format!(
                "provider {provider} does not expose a configured ANN traversal for AUTO \
                 to qualify"
            )));
        }
        policy
            .verify_identity(&crate::dense_policy::LiveIdentity {
                provider_backend: facts.provider.clone(),
                scoring_fingerprint: facts.scoring_fingerprint.clone(),
                corpus_generation: facts.generation,
                corpus_rows: facts.rows,
                dimensions: facts.dimensions,
            })
            .map_err(Status::failed_precondition)?;
        let selectivity = self
            .dense_filter_selectivity(key.filters, facts.rows)
            .await?;
        let hit = policy
            .qualify(crate::dense_policy::RequestKey {
                k: key.k,
                candidate_depth: key.candidate_depth,
                filter_selectivity_ppm: selectivity,
            })
            .map_err(Status::failed_precondition)?;
        check_enumerate_regime(&hit, facts, selectivity)?;
        check_traversal_regimes(&hit, facts)?;
        let reason = auto_reason(&hit, policy.measured_queries(), selectivity);
        Ok((hit, selectivity, reason))
    }
}

/// The recall a point claims is served by the regimes that measured it: a
/// provider that chooses a filter regime takes the point's enumerate
/// threshold, and a point cannot name one for a provider that has none.
fn check_enumerate_regime(
    hit: &crate::dense_policy::Qualified,
    facts: &DenseProviderFacts,
    selectivity: u32,
) -> Result<(), Status> {
    let provider = &facts.provider;
    let point = hit.point;
    match point.enumerate_max_rows {
        Some(threshold) if !facts.enumerates => Err(Status::failed_precondition(format!(
            "dense execution policy {:?} point k={} depth {} names \
             enumerate_max_rows {threshold}, but provider {provider} has no \
             enumerate regime (it does not advertise mask_enumeration)",
            hit.policy_id, point.k, point.candidates
        ))),
        None if facts.enumerates && selectivity != crate::dense_policy::UNFILTERED_PPM => {
            Err(Status::failed_precondition(format!(
                "dense execution policy {:?} point k={} depth {} selectivity \
                 {}..={} ppm names no enumerate_max_rows; provider {provider} answers \
                 a filter in a regime chosen by that threshold, and a measured point \
                 is served only under the threshold it was measured with",
                hit.policy_id,
                point.k,
                point.candidates,
                point.selectivity_min_ppm,
                point.selectivity_max_ppm
            )))
        }
        _ => Ok(()),
    }
}

/// Refuse a point that names a traversal regime (patience, finish settle,
/// floor pruning) the live provider does not advertise.
fn check_traversal_regimes(
    hit: &crate::dense_policy::Qualified,
    facts: &DenseProviderFacts,
) -> Result<(), Status> {
    let provider = &facts.provider;
    let point = hit.point;
    if let (Some(sat_ppm), Some(hops)) = (point.patience_saturation_ppm, point.patience_hops) {
        if !facts.supports_patience {
            return Err(Status::failed_precondition(format!(
                "dense execution policy {:?} point k={} depth {} names patience \
                 (saturation {sat_ppm} ppm, {hops} hops), but provider {provider} \
                 does not support patience early stopping (it does not advertise \
                 patience_early_stop)",
                hit.policy_id, point.k, point.candidates
            )));
        }
    }
    if let Some(settle_us) = point.finish_settle_us {
        if !facts.supports_stream {
            return Err(Status::failed_precondition(format!(
                "dense execution policy {:?} point k={} depth {} names \
                 finish_settle_us {settle_us}, but provider {provider} does not \
                 support streaming search (it does not advertise candidate_stream)",
                hit.policy_id, point.k, point.candidates
            )));
        }
    }
    if let (Some(greediness_ppm), Some(clamp_min)) =
        (point.floor_greediness_ppm, point.floor_clamp_min)
    {
        if !facts.supports_floor_prune {
            return Err(Status::failed_precondition(format!(
                "dense execution policy {:?} point k={} depth {} names floor pruning \
                 (greediness {greediness_ppm} ppm, clamp_min {clamp_min}), but \
                 provider {provider} does not support graph floor pruning (it does \
                 not advertise floor_pruning)",
                hit.policy_id, point.k, point.candidates
            )));
        }
    }
    Ok(())
}

/// The planner reason for an AUTO request the policy qualified, naming the
/// point and every regime it was measured under.
fn auto_reason(
    hit: &crate::dense_policy::Qualified,
    measured_queries: u32,
    selectivity: u32,
) -> String {
    let point = hit.point;
    let reason = format!(
        "AUTO selected the provider's configured approximate traversal through \
         policy {:?} point k={} selectivity {}..={} ppm depth {} (measured recall {} \
         ppm over {} queries) at live selectivity {selectivity} ppm",
        hit.policy_id,
        point.k,
        point.selectivity_min_ppm,
        point.selectivity_max_ppm,
        point.candidates,
        point.measured_recall_ppm,
        measured_queries
    );
    let reason = match point.enumerate_max_rows {
        Some(threshold) => {
            format!("{reason}; enumerate threshold {threshold} rows a segment")
        }
        None => reason,
    };
    let reason = match (point.patience_saturation_ppm, point.patience_hops) {
        (Some(sat_ppm), Some(hops)) => {
            format!("{reason}; patience {sat_ppm} ppm over {hops} hops")
        }
        _ => reason,
    };
    let reason = match point.finish_settle_us {
        Some(settle_us) => format!("{reason}; finish settle {settle_us} us"),
        None => reason,
    };
    match (point.floor_greediness_ppm, point.floor_clamp_min) {
        (Some(greediness_ppm), Some(clamp_min)) => {
            format!("{reason}; floor prune {greediness_ppm} ppm (clamp {clamp_min})")
        }
        _ => reason,
    }
}
