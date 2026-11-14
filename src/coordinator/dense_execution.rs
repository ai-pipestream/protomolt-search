//! Dense provider selection, quality checks, and exact rescoring.

use super::*;

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
        let (
            provider,
            scoring_fingerprint,
            quality,
            exhaustive,
            enumerates,
            supports_patience,
            supports_stream,
            supports_floor_prune,
            dimensions,
            rows,
            generation,
        ) = if let Some(identity) = self.clustered_quality_identity().await? {
            (
                "clustered-turbovec".to_string(),
                identity.scoring_fingerprint,
                crate::pb::VectorQualityContract::ExhaustiveNativeScore,
                true,
                false,
                false,
                false,
                false,
                identity.dimensions,
                identity.rows,
                identity.topology_generation,
            )
        } else {
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

            let mut provider = None;
            let mut fingerprint = None;
            let mut quality = None;
            let mut exhaustive = None;
            // Every shard is one provider under one fingerprint, so
            // one that lacks the regime means none can be relied on.
            let mut enumerates = true;
            let mut supports_patience = true;
            let mut supports_stream = true;
            let mut supports_floor_prune = true;
            let mut dimensions = None;
            let mut rows: u64 = 0;
            for task in tasks {
                let (addr, backend) = task.await.map_err(|error| {
                    Status::internal(format!("dense execution preflight failed: {error}"))
                })??;
                rows = rows.checked_add(backend.num_vectors).ok_or_else(|| {
                    Status::internal("dense execution preflight row count overflow")
                })?;
                let descriptor = backend.descriptor.ok_or_else(|| {
                    Status::failed_precondition(
                        "dense execution preflight found an unconfigured vector shard",
                    )
                })?;
                self.shard_vector_rows
                    .lock()
                    .expect("shard vector row cache poisoned")
                    .insert(addr, backend.num_vectors);
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
                let shard_quality =
                    crate::pb::VectorQualityContract::try_from(descriptor.quality_contract)
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
                let direction = crate::pb::VectorScoreDirection::try_from(
                    descriptor.score_direction,
                )
                .map_err(|_| {
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
                if !descriptor
                    .capabilities
                    .iter()
                    .any(|capability| capability == "batch_query")
                {
                    return Err(Status::failed_precondition(format!(
                        "vector provider {} does not advertise batch_query",
                        descriptor.backend_kind
                    )));
                }
                let shard_exhaustive = shard_quality
                    == crate::pb::VectorQualityContract::ExhaustiveNativeScore
                    && descriptor
                        .capabilities
                        .iter()
                        .any(|capability| capability == "exhaustive_completion");
                enumerates &= descriptor
                    .capabilities
                    .iter()
                    .any(|capability| capability == "mask_enumeration");
                supports_patience &= descriptor
                    .capabilities
                    .iter()
                    .any(|capability| capability == "patience_early_stop");
                supports_stream &= descriptor
                    .capabilities
                    .iter()
                    .any(|capability| capability == "candidate_stream");
                supports_floor_prune &= descriptor
                    .capabilities
                    .iter()
                    .any(|capability| capability == "floor_pruning");

                for (held, actual, mismatch) in [
                    (
                        &mut provider,
                        descriptor.backend_kind,
                        "mixed vector providers",
                    ),
                    (
                        &mut fingerprint,
                        descriptor.scoring_fingerprint,
                        "mixed scoring fingerprints",
                    ),
                ] {
                    match held {
                        Some(value) if value != &actual => {
                            return Err(Status::failed_precondition(format!(
                                "dense execution preflight found {mismatch}"
                            )))
                        }
                        None => *held = Some(actual),
                        _ => {}
                    }
                }
                match quality {
                    Some(value) if value != shard_quality => {
                        return Err(Status::failed_precondition(
                            "dense execution preflight found mixed quality contracts",
                        ))
                    }
                    None => quality = Some(shard_quality),
                    _ => {}
                }
                match exhaustive {
                    Some(value) if value != shard_exhaustive => {
                        return Err(Status::failed_precondition(
                            "dense execution preflight found mixed completion capabilities",
                        ))
                    }
                    None => exhaustive = Some(shard_exhaustive),
                    _ => {}
                }
                match dimensions {
                    Some(value) if value != descriptor.dim => {
                        return Err(Status::failed_precondition(
                            "dense execution preflight found mixed vector dimensions",
                        ))
                    }
                    None => dimensions = Some(descriptor.dim),
                    _ => {}
                }
            }
            let enumerates = enumerates && provider.is_some();
            let supports_patience = supports_patience && provider.is_some();
            let supports_stream = supports_stream && provider.is_some();
            let supports_floor_prune = supports_floor_prune && provider.is_some();
            (
                provider.unwrap_or_default(),
                fingerprint.unwrap_or_default(),
                quality.unwrap_or(crate::pb::VectorQualityContract::Unspecified),
                exhaustive.unwrap_or(false),
                enumerates,
                supports_patience,
                supports_stream,
                supports_floor_prune,
                dimensions.unwrap_or_default(),
                rows,
                self.topology_generation,
            )
        };

        if dimensions as usize != query_dim {
            return Err(Status::failed_precondition(format!(
                "dense query dimension {query_dim} does not match live provider dimension {dimensions}"
            )));
        }

        let exact_available =
            quality == crate::pb::VectorQualityContract::ExhaustiveNativeScore && exhaustive;
        let mut qualified: Option<(crate::dense_policy::Qualified, u32)> = None;
        let (resolved, planner_reason) = match requested {
            crate::pb::DenseExecutionMode::Unspecified => {
                if !exact_available {
                    return Err(Status::failed_precondition(format!(
                        "unspecified dense execution preserves the exact contract, but provider {provider} advertises {quality:?} without exhaustive completion; request ANN explicitly or install an exact backend"
                    )));
                }
                (
                    crate::pb::DenseExecutionMode::Exact,
                    "legacy unspecified mode preserves exact traversal".to_string(),
                )
            }
            crate::pb::DenseExecutionMode::Exact => {
                if !exact_available {
                    return Err(Status::failed_precondition(format!(
                        "EXACT dense execution requires exhaustive native scoring and completion, but provider {provider} advertises {quality:?}"
                    )));
                }
                (
                    crate::pb::DenseExecutionMode::Exact,
                    "caller required exact traversal".to_string(),
                )
            }
            crate::pb::DenseExecutionMode::Ann => {
                if exact_available {
                    return Err(Status::failed_precondition(format!(
                        "ANN dense execution was requested, but provider {provider} exposes only exhaustive traversal"
                    )));
                }
                if !matches!(
                    quality,
                    crate::pb::VectorQualityContract::ConfiguredAnn
                        | crate::pb::VectorQualityContract::ProbabilisticBound
                ) {
                    return Err(Status::failed_precondition(format!(
                        "provider {provider} does not expose a configured ANN traversal"
                    )));
                }
                (
                    crate::pb::DenseExecutionMode::Ann,
                    "caller accepted the provider's configured approximate traversal".to_string(),
                )
            }
            crate::pb::DenseExecutionMode::Auto if exact_available => (
                crate::pb::DenseExecutionMode::Exact,
                "AUTO selected exact because the live provider proves exhaustive completion"
                    .to_string(),
            ),
            // AUTO on a provider without exhaustive completion goes only
            // through the generation-bound policy: identity first, then
            // the exact request key. Nothing is interpolated or defaulted.
            crate::pb::DenseExecutionMode::Auto => {
                let policy = self.dense_execution_policy.as_ref().ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "AUTO has no dense execution policy for provider {provider} ({quality:?}); \
                         install one with --dense-execution-policy, or use ANN explicitly to \
                         accept its configured traversal"
                    ))
                })?;
                if !matches!(
                    quality,
                    crate::pb::VectorQualityContract::ConfiguredAnn
                        | crate::pb::VectorQualityContract::ProbabilisticBound
                ) {
                    return Err(Status::failed_precondition(format!(
                        "provider {provider} does not expose a configured ANN traversal for AUTO \
                         to qualify"
                    )));
                }
                policy
                    .verify_identity(&crate::dense_policy::LiveIdentity {
                        provider_backend: provider.clone(),
                        scoring_fingerprint: scoring_fingerprint.clone(),
                        corpus_generation: generation,
                        corpus_rows: rows,
                        dimensions,
                    })
                    .map_err(Status::failed_precondition)?;
                let selectivity = self.dense_filter_selectivity(key.filters, rows).await?;
                let hit = policy
                    .qualify(crate::dense_policy::RequestKey {
                        k: key.k,
                        candidate_depth: key.candidate_depth,
                        filter_selectivity_ppm: selectivity,
                    })
                    .map_err(Status::failed_precondition)?;
                let point = hit.point;
                // The recall a point claims is served by the regimes that
                // measured it: a provider that chooses a filter regime
                // takes the point's enumerate threshold, and a point
                // cannot name one for a provider that has none.
                match point.enumerate_max_rows {
                    Some(threshold) if !enumerates => {
                        return Err(Status::failed_precondition(format!(
                            "dense execution policy {:?} point k={} depth {} names \
                             enumerate_max_rows {threshold}, but provider {provider} has no \
                             enumerate regime (it does not advertise mask_enumeration)",
                            hit.policy_id, point.k, point.candidates
                        )));
                    }
                    None if enumerates && selectivity != crate::dense_policy::UNFILTERED_PPM => {
                        return Err(Status::failed_precondition(format!(
                            "dense execution policy {:?} point k={} depth {} selectivity \
                             {}..={} ppm names no enumerate_max_rows; provider {provider} answers \
                             a filter in a regime chosen by that threshold, and a measured point \
                             is served only under the threshold it was measured with",
                            hit.policy_id,
                            point.k,
                            point.candidates,
                            point.selectivity_min_ppm,
                            point.selectivity_max_ppm
                        )));
                    }
                    _ => {}
                }
                if let (Some(sat_ppm), Some(hops)) =
                    (point.patience_saturation_ppm, point.patience_hops)
                {
                    if !supports_patience {
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
                    if !supports_stream {
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
                    if !supports_floor_prune {
                        return Err(Status::failed_precondition(format!(
                            "dense execution policy {:?} point k={} depth {} names floor pruning \
                             (greediness {greediness_ppm} ppm, clamp_min {clamp_min}), but \
                             provider {provider} does not support graph floor pruning (it does \
                             not advertise floor_pruning)",
                            hit.policy_id, point.k, point.candidates
                        )));
                    }
                }
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
                    policy.measured_queries()
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
                let reason = match (point.floor_greediness_ppm, point.floor_clamp_min) {
                    (Some(greediness_ppm), Some(clamp_min)) => {
                        format!("{reason}; floor prune {greediness_ppm} ppm (clamp {clamp_min})")
                    }
                    _ => reason,
                };
                qualified = Some((hit, selectivity));
                (crate::pb::DenseExecutionMode::Ann, reason)
            }
        };

        let mut outcome = crate::pb::DenseExecutionOutcome {
            requested_mode: requested as i32,
            resolved_mode: resolved as i32,
            provider_backend: provider,
            quality_contract: quality as i32,
            scoring_fingerprint,
            exhaustive_completion: resolved == crate::pb::DenseExecutionMode::Exact,
            planner_reason,
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
        if let Some((hit, selectivity)) = qualified {
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
        Ok(outcome)
    }

    pub(super) async fn dense_filter_selectivity(
        &self,
        filters: Option<&RequestFilters>,
        rows: u64,
    ) -> Result<u32, Status> {
        let caller_filtered = filters.is_some_and(|f| !f.geo.is_empty() || f.tree.is_some());
        if !caller_filtered && self.document_visibility.is_none() {
            return Ok(crate::dense_policy::UNFILTERED_PPM);
        }
        // Both membership passes must describe the same physical read. A
        // standalone planner has no public Query envelope to pin them for it.
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.dense_filter_selectivity(filters, rows)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let vectors = self
            .vector_membership(self.vector_read_field.as_deref().unwrap_or(""))
            .await?;
        let admitted = if caller_filtered {
            let documents = self
                .filter_membership(filters.expect("caller filter"))
                .await?;
            vectors.ids.intersection(&documents.ids).count()
        } else {
            vectors.ids.len()
        };
        Ok(crate::dense_policy::selectivity_ppm(admitted as u64, rows))
    }
}

impl CoordinatorServiceImpl {
    /// One provider identity across the shard fleet, or a refusal that
    /// names the mismatch (`docs/mmap-vectors.md`): every reachable shard
    /// must advertise the same backend kind, scoring fingerprint, and
    /// dimension before the coordinator scores anything across them.
    /// With `require_configured`, a shard without a vector backend is a
    /// refusal too; without it (ingest, health), unconfigured shards are
    /// skipped and an all-unconfigured fleet is an empty identity.
    pub(crate) async fn fleet_vector_identity(
        &self,
        require_configured: bool,
    ) -> Result<FleetVectorIdentity, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let mut client = self.node_client(addr)?;
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                client
                    .get_vector_backend(crate::pb::GetVectorBackendRequest {})
                    .await
                    .map(|response| (shard, addr, response.into_inner()))
            }));
        }
        let mut identity: Option<FleetVectorIdentity> = None;
        let mut first: Option<(usize, String)> = None;
        for task in tasks {
            let (shard, addr, backend) = task.await.map_err(|error| {
                Status::internal(format!("provider preflight failed: {error}"))
            })??;
            let Some(descriptor) = backend.descriptor else {
                if require_configured {
                    return Err(Status::failed_precondition(format!(
                        "provider preflight: shard {shard} ({addr}) has no vector backend \
                         configured"
                    )));
                }
                continue;
            };
            self.shard_vector_rows
                .lock()
                .expect("shard vector row cache poisoned")
                .insert(addr.clone(), backend.num_vectors);
            let seen = FleetVectorIdentity {
                provider: descriptor.backend_kind.clone(),
                scoring_fingerprint: descriptor.scoring_fingerprint.clone(),
                dimensions: descriptor.dim,
                rows: backend.num_vectors,
            };
            match &mut identity {
                None => {
                    identity = Some(seen);
                    first = Some((shard, addr));
                }
                Some(held) => {
                    let (first_shard, first_addr) =
                        first.as_ref().expect("identity and first are set together");
                    if held.provider != seen.provider
                        || held.scoring_fingerprint != seen.scoring_fingerprint
                        || held.dimensions != seen.dimensions
                    {
                        return Err(Status::failed_precondition(format!(
                            "provider preflight: shard {first_shard} ({first_addr}) scores \
                             under {}/{} dim {}, but shard {shard} ({addr}) under {}/{} dim {}; \
                             a fleet scores in one space, so nothing is searched until every \
                             shard serves the same provider state",
                            held.provider,
                            held.scoring_fingerprint,
                            held.dimensions,
                            seen.provider,
                            seen.scoring_fingerprint,
                            seen.dimensions
                        )));
                    }
                    held.rows = held
                        .rows
                        .checked_add(seen.rows)
                        .ok_or_else(|| Status::internal("provider preflight row count overflow"))?;
                }
            }
        }
        Ok(identity.unwrap_or_default())
    }

    /// The clustered backend's live identity, when one is configured.
    #[cfg(feature = "net")]
    pub(super) async fn clustered_quality_identity(
        &self,
    ) -> Result<Option<crate::clustered_turbovec::ClusteredQualityIdentity>, Status> {
        match &self.clustered_vectors {
            Some(clustered) => clustered.quality_identity().await.map(Some),
            None => Ok(None),
        }
    }

    #[cfg(not(feature = "net"))]
    pub(super) async fn clustered_quality_identity(
        &self,
    ) -> Result<Option<crate::clustered_turbovec::ClusteredQualityIdentity>, Status> {
        Ok(None)
    }

    /// `DENSE_EXECUTION_MODE_AUTO` with FP32 rerank, no policy, and no
    /// `selection_k`: resolve through the installed profile's default
    /// target exactly as an explicit `DenseQualityPolicy` naming it would
    /// (`docs/dense-quality-profile.md`). No profile, or a profile without
    /// a default, refuses by name rather than running at `selection_k = k`.
    pub(crate) async fn resolve_dense_quality_default(
        &self,
        k: u32,
        query_dim: usize,
    ) -> Result<crate::quality::DenseQualityResolution, Status> {
        const NEEDS: &str = "AUTO with FP32 rerank needs a measured quality profile with \
                             default_target_recall_ppm, or an explicit DenseQualityPolicy or \
                             selection_k";
        let profile = self.dense_quality_profile.as_ref().ok_or_else(|| {
            Status::failed_precondition(format!(
                "{NEEDS}; this coordinator has no --dense-quality-profile"
            ))
        })?;
        let target = profile.default_target_recall_ppm().ok_or_else(|| {
            Status::failed_precondition(format!(
                "{NEEDS}; profile {:?} carries no default_target_recall_ppm",
                profile.profile_id()
            ))
        })?;
        self.resolve_dense_quality(
            k,
            query_dim,
            &crate::pb::DenseQualityPolicy {
                target_recall_ppm: target,
                max_candidates: 0,
                required_profile_fingerprint: String::new(),
            },
        )
        .await
    }

    /// Resolve and prove one measured dense quality request against the live
    /// provider and product exact-row generation. Drift is a hard failure.
    pub(crate) async fn resolve_dense_quality(
        &self,
        k: u32,
        query_dim: usize,
        policy: &crate::pb::DenseQualityPolicy,
    ) -> Result<crate::quality::DenseQualityResolution, Status> {
        let profile = self.dense_quality_profile.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "dense quality policy requested but this coordinator has no --dense-quality-profile",
            )
        })?;
        let resolution = profile
            .resolve(
                k,
                policy.target_recall_ppm,
                &policy.required_profile_fingerprint,
                policy.max_candidates,
            )
            .map_err(Status::invalid_argument)?;
        if resolution.selection_k > self.knobs.max_k() {
            return Err(Status::failed_precondition(format!(
                "quality profile resolves selection_k={} above coordinator max_k={}; raise --max-k or measure a bounded policy",
                resolution.selection_k, self.knobs.max_k()
            )));
        }
        if resolution.dimensions as usize != query_dim {
            return Err(Status::failed_precondition(format!(
                "quality profile dimension {} does not match query dimension {query_dim}",
                resolution.dimensions
            )));
        }

        let (provider, scoring_fingerprint, rows, generation, dimensions) = if let Some(identity) =
            self.clustered_quality_identity().await?
        {
            let mut tasks = Vec::with_capacity(self.node_addrs.len());
            for addr in &self.node_addrs {
                let mut client = self.node_client(addr)?;
                tasks.push(tokio::spawn(async move {
                    client
                        .health(HealthRequest {})
                        .await
                        .map(|reply| reply.into_inner())
                }));
            }
            let mut exact_rows = 0u64;
            for task in tasks {
                let health = task.await.map_err(|error| {
                    Status::internal(format!("quality exact-row preflight failed: {error}"))
                })??;
                if !health.exact_vectors_available {
                    return Err(Status::failed_precondition(format!(
                        "clustered quality profile requires aligned product-owned exact rows, but product shard at slot_offset={} has none",
                        health.slot_offset
                    )));
                }
                if health.deleted_docs != 0 {
                    return Err(Status::failed_precondition(format!(
                        "clustered quality profile was measured on an all-live generation, but product shard at slot_offset={} has {} tombstoned rows; compact and remeasure",
                        health.slot_offset, health.deleted_docs
                    )));
                }
                exact_rows = exact_rows
                    .checked_add(health.exact_vector_rows)
                    .ok_or_else(|| Status::internal("quality exact-row count overflow"))?;
            }
            if exact_rows != identity.rows {
                return Err(Status::failed_precondition(format!(
                    "clustered vector collection has {} rows but product shards own {exact_rows} exact rows",
                    identity.rows
                )));
            }
            (
                "clustered-turbovec".to_string(),
                identity.scoring_fingerprint,
                identity.rows,
                identity.topology_generation,
                identity.dimensions,
            )
        } else {
            let mut tasks = Vec::with_capacity(self.node_addrs.len());
            for addr in &self.node_addrs {
                let mut client = self.node_client(addr)?;
                tasks.push(tokio::spawn(async move {
                    let backend = client
                        .get_vector_backend(crate::pb::GetVectorBackendRequest {})
                        .await?
                        .into_inner();
                    let health = client.health(HealthRequest {}).await?.into_inner();
                    Ok::<_, Status>((backend, health))
                }));
            }
            let mut provider = None;
            let mut fingerprint = None;
            let mut rows = 0u64;
            let mut dimensions = None;
            for task in tasks {
                let (backend, health) = task.await.map_err(|error| {
                    Status::internal(format!("quality preflight task failed: {error}"))
                })??;
                let descriptor = backend.descriptor.ok_or_else(|| {
                    Status::failed_precondition(
                        "quality preflight found a shard without a vector descriptor",
                    )
                })?;
                if !health.exact_vectors_available {
                    return Err(Status::failed_precondition(format!(
                            "quality profile requires aligned exact rows, but shard at slot_offset={} has none",
                            health.slot_offset
                        )));
                }
                if health.deleted_docs != 0 {
                    return Err(Status::failed_precondition(format!(
                        "quality profile was measured on an all-live generation, but shard at slot_offset={} has {} tombstoned rows; compact and remeasure",
                        health.slot_offset, health.deleted_docs
                    )));
                }
                match &provider {
                    Some(held) if held != &descriptor.backend_kind => {
                        return Err(Status::failed_precondition(
                            "quality preflight found mixed vector providers",
                        ))
                    }
                    None => provider = Some(descriptor.backend_kind.clone()),
                    _ => {}
                }
                match &fingerprint {
                    Some(held) if held != &descriptor.scoring_fingerprint => {
                        return Err(Status::failed_precondition(
                            "quality preflight found mixed scoring fingerprints",
                        ))
                    }
                    None => fingerprint = Some(descriptor.scoring_fingerprint.clone()),
                    _ => {}
                }
                match dimensions {
                    Some(held) if held != descriptor.dim => {
                        return Err(Status::failed_precondition(
                            "quality preflight found mixed vector dimensions",
                        ))
                    }
                    None => dimensions = Some(descriptor.dim),
                    _ => {}
                }
                rows = rows
                    .checked_add(backend.num_vectors)
                    .ok_or_else(|| Status::internal("quality preflight row count overflow"))?;
            }
            (
                provider.unwrap_or_default(),
                fingerprint.unwrap_or_default(),
                rows,
                self.topology_generation,
                dimensions.unwrap_or_default(),
            )
        };

        for (name, expected, actual) in [
            (
                "provider_backend",
                resolution.provider_backend.as_str(),
                provider.as_str(),
            ),
            (
                "scoring_fingerprint",
                resolution.scoring_fingerprint.as_str(),
                scoring_fingerprint.as_str(),
            ),
        ] {
            if expected != actual {
                return Err(Status::failed_precondition(format!(
                    "quality profile {name} {expected:?} does not match live {actual:?}"
                )));
            }
        }
        if resolution.corpus_rows != rows
            || resolution.corpus_generation != generation
            || resolution.dimensions != dimensions
        {
            return Err(Status::failed_precondition(format!(
                "quality profile generation mismatch: profile rows/generation/dim={}/{}/{}, live={rows}/{generation}/{dimensions}",
                resolution.corpus_rows,
                resolution.corpus_generation,
                resolution.dimensions
            )));
        }
        Ok(resolution)
    }
}

pub(crate) struct ExactRerankScores {
    pub scores: HashMap<u64, f32>,
    pub rows: u64,
    pub logical_bytes: u64,
    pub pages_touched: u64,
    pub tasks: u32,
}

/// The provider-mismatch line of a cluster health report: empty when
/// every reachable, configured shard scores under one backend kind and
/// scoring fingerprint, else every distinct pair with the shards that
/// serve it.
pub(super) fn provider_mismatch_of(targets: &[ShardHealth]) -> String {
    let mut seen: Vec<((String, String), Vec<String>)> = Vec::new();
    for target in targets {
        let Some(health) = &target.health else {
            continue;
        };
        if health.vector_backend.is_empty() {
            continue;
        }
        let key = (
            health.vector_backend.clone(),
            health.scoring_fingerprint.clone(),
        );
        let who = format!("shard {} ({})", target.shard, target.addr);
        match seen.iter_mut().find(|(k, _)| *k == key) {
            Some((_, shards)) => shards.push(who),
            None => seen.push((key, vec![who])),
        }
    }
    if seen.len() <= 1 {
        return String::new();
    }
    seen.iter()
        .map(|((kind, fingerprint), shards)| format!("{kind}/{fingerprint}: {}", shards.join(", ")))
        .collect::<Vec<_>>()
        .join("; ")
}

/// One provider identity the shard fleet agrees on
/// (`CoordinatorServiceImpl::fleet_vector_identity`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FleetVectorIdentity {
    pub provider: String,
    pub scoring_fingerprint: String,
    pub dimensions: u32,
    pub rows: u64,
}

/// The request side of a dense execution policy key
/// (`docs/dense-execution-policy.md`): the requested `k` as sent (0 is
/// refused by the policy, not defaulted), the candidate depth the
/// request named (`selection_k`, 0 when none), and the request's
/// filters, whose live selectivity the coordinator measures when AUTO
/// consults the policy.
#[derive(Debug, Clone, Copy)]
pub struct DenseRequestKey<'a> {
    pub k: u32,
    pub candidate_depth: u32,
    pub filters: Option<&'a RequestFilters>,
}

impl CoordinatorServiceImpl {
    /// Score a fixed candidate set against the product-owned original FP32
    /// rows. Every requested id must resolve exactly once across the product
    /// shards. Selection may come from the embedded or clustered provider;
    /// stable labels route back to these product-owned rows in either case.
    pub(crate) async fn exact_vector_scores(
        &self,
        vector: &[f32],
        ids: &[u64],
        field: &str,
    ) -> Result<super::ExactRerankScores, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(field)?;
        }
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.exact_vector_scores(vector, ids, field)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        if vector.is_empty() {
            return Err(Status::invalid_argument(
                "FP32 rerank needs a non-empty query vector",
            ));
        }
        if ids.is_empty() && field.is_empty() && self.document_visibility.is_none() {
            return Ok(ExactRerankScores {
                scores: HashMap::new(),
                rows: 0,
                logical_bytes: 0,
                pages_touched: 0,
                tasks: 0,
            });
        }
        let mut requested = ids.to_vec();
        requested.sort_unstable();
        requested.dedup();
        let logical_bytes = (requested.len() as u64)
            .checked_mul(vector.len() as u64)
            .and_then(|bytes| bytes.checked_mul(4))
            .ok_or_else(|| Status::resource_exhausted("FP32 rerank byte count overflow"))?;
        if logical_bytes > self.max_rerank_bytes {
            return Err(Status::resource_exhausted(format!(
                "FP32 rerank needs {logical_bytes} logical row bytes for {} candidates at dim {}, above coordinator max_rerank_bytes={}",
                requested.len(),
                vector.len(),
                self.max_rerank_bytes
            )));
        }
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let claim = self.query_read_versions.as_ref().expect("pinned reads")[shard];
            let request = ExactVectorRescoreRequest {
                field: field.into(),
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                vector: vector.to_vec(),
                candidate_ids: requested.clone(),
                max_logical_bytes: self.max_rerank_bytes,
            };
            let mut client = self.node_client(addr)?;
            let deadline = self.limits.shard_deadline;
            tasks.push(tokio::spawn(async move {
                let call = client.exact_vector_rescore(request);
                match deadline {
                    Some(limit) => tokio::time::timeout(limit, call)
                        .await
                        .map_err(|_| {
                            Status::deadline_exceeded(
                                "exact vector rescore shard deadline exceeded",
                            )
                        })?
                        .map(tonic::Response::into_inner),
                    None => call.await.map(tonic::Response::into_inner),
                }
            }));
        }
        let mut scores = HashMap::with_capacity(requested.len());
        let mut observed_bytes = 0u64;
        let mut pages_touched = 0u64;
        let mut worker_tasks = 0u32;
        let mut held_binding = None;
        for (shard, task) in tasks.into_iter().enumerate() {
            let response = task.await.map_err(|e| {
                Status::internal(format!("exact vector rescore task failed: {e}"))
            })??;
            Self::check_vector_binding(field, response.vector_binding.as_ref(), &mut held_binding)?;
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            observed_bytes = observed_bytes
                .checked_add(response.logical_bytes)
                .ok_or_else(|| Status::internal("exact rerank byte metrics overflow"))?;
            pages_touched = pages_touched
                .checked_add(response.pages_touched)
                .ok_or_else(|| Status::internal("exact rerank page metrics overflow"))?;
            worker_tasks = worker_tasks.saturating_add(response.tasks);
            for hit in response.hits {
                if requested.binary_search(&hit.doc_id).is_err()
                    || !hit.score.is_finite()
                    || scores.insert(hit.doc_id, hit.score).is_some()
                {
                    return Err(Status::failed_precondition(format!(
                        "FP32 rerank returned an unrequested, duplicate or invalid score for candidate {}",
                        hit.doc_id
                    )));
                }
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        if let Some(missing) = requested.iter().find(|id| !scores.contains_key(id)) {
            return Err(Status::failed_precondition(format!(
                "FP32 rerank candidate {missing} has no exact-vector row on any product shard"
            )));
        }
        if observed_bytes != logical_bytes {
            return Err(Status::failed_precondition(format!(
                "FP32 rerank shards reported {observed_bytes} logical bytes, expected {logical_bytes}; exact-row dimensions or ownership disagree"
            )));
        }
        Ok(ExactRerankScores {
            scores,
            rows: requested.len() as u64,
            logical_bytes,
            pages_touched,
            tasks: worker_tasks,
        })
    }
}
