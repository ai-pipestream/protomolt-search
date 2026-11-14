//! The deterministic tier plan (§4 and §5) and its serialized response.

use super::*;

impl PlanTiersResponse {
    /// Deterministic serialized form for the byte-identity obligation of
    /// §4's proof test: two planner instances given the same snapshot
    /// must emit identical bytes. This is the implementation's own
    /// canonical form (not a §5-hashed value).
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.plan_digest);
        out.extend_from_slice(&self.policy_fingerprint);
        put_u64(&mut out, self.observation_epoch);
        put_u64(&mut out, self.control_revision);
        put_u32(&mut out, self.placements.len() as u32);
        for placement in &self.placements {
            placement.fragment.put_canonical(&mut out);
            match &placement.classified_tier {
                Some(tier) => {
                    out.push(1);
                    put_str(&mut out, tier);
                }
                None => out.push(0),
            }
            match &placement.unclassified_reason {
                Some(reason) => {
                    out.push(1);
                    put_str(&mut out, reason);
                }
                None => out.push(0),
            }
            put_u64(&mut out, placement.rate_nanos);
            put_u64(&mut out, placement.rows);
            put_u64(&mut out, placement.resident_bytes);
            put_u64(&mut out, placement.last_scanned_unix_ms);
            put_u32(&mut out, placement.complete_replicas.len() as u32);
            for copy in &placement.complete_replicas {
                put_copy(&mut out, copy);
            }
            put_u32(&mut out, placement.reporters.len() as u32);
            for reporter in &placement.reporters {
                put_str(&mut out, &reporter.node_id);
                put_str(&mut out, &reporter.shard);
                put_u64(&mut out, reporter.scans_observed);
                match reporter.queue_wait_p50_us {
                    Some(v) => {
                        out.push(1);
                        put_u64(&mut out, v);
                    }
                    None => out.push(0),
                }
                match reporter.queue_wait_p99_us {
                    Some(v) => {
                        out.push(1);
                        put_u64(&mut out, v);
                    }
                    None => out.push(0),
                }
                put_u32(&mut out, reporter.samples);
                put_u64(&mut out, reporter.last_scanned_unix_ms);
            }
        }
        put_u32(&mut out, self.moves.len() as u32);
        for mv in &self.moves {
            mv.fragment.put_canonical(&mut out);
            put_copy(&mut out, &mv.source);
            put_str(&mut out, &mv.destination_node);
            put_str(&mut out, &mv.from_tier);
            put_str(&mut out, &mv.to_tier);
            put_u64(&mut out, mv.bytes);
            out.extend_from_slice(&mv.plan_digest);
            put_u64(&mut out, mv.control_revision);
            put_u64(&mut out, mv.observation_epoch);
            out.extend_from_slice(&mv.policy_fingerprint);
        }
        put_u32(&mut out, self.refusals.len() as u32);
        for refusal in &self.refusals {
            put_str(&mut out, refusal);
        }
        out
    }
}

/// §5's exact rate: `floor(S · 10^12 / (W · B))` in `u128`. With §3's
/// bounds the arithmetic cannot overflow; the caller guarantees `B > 0`.
pub(super) fn rate_nanos(scans: u128, window_ms: u64, resident_bytes: u128) -> u64 {
    let numerator = scans * 1_000_000_000_000u128;
    let denominator = (window_ms as u128) * resident_bytes;
    u64::try_from(numerator / denominator).unwrap_or(u64::MAX)
}

/// §3/§5's fan-in bound.
pub(super) fn ensure_fan_in(count: usize) -> Result<(), String> {
    if count > MAX_REPORTS_PER_FRAGMENT {
        return Err(format!(
            "plan_tiers: fragment aggregates {count} reports, past the 2^20 fan-in bound"
        ));
    }
    Ok(())
}

/// §4: the deterministic dry run. Pure function of the validated
/// snapshot; the same snapshot yields the same response, bitwise.
pub fn plan_tiers(snapshot: &TierSnapshot) -> Result<PlanTiersResponse, String> {
    snapshot.policy.validate()?;
    let t = snapshot.planning_instant_unix_ms;
    let (w0, w1) = snapshot.current_cohort()?;
    let skew_horizon = t
        .checked_add(snapshot.clock_skew_bound_ms)
        .ok_or_else(|| "plan_tiers: planning instant plus the skew bound overflows".to_string())?;

    // Index the current-cohort reports of covered fragments by their full
    // identity — resource, declaration, generation, leaf, shard, node —
    // and apply the skew rule to every stored report the plan could read.
    let covered: std::collections::BTreeSet<FragmentKey> = snapshot
        .fragments
        .iter()
        .map(|f| (f.partition.clone(), f.topology_generation, f.leaf.clone()))
        .collect();
    let mut reports: BTreeMap<FragmentKey, FragmentReports> = BTreeMap::new();
    for obs in &snapshot.observations {
        if obs.window_end_unix_ms > skew_horizon {
            return Err(format!(
                "plan_tiers: observation from node {} ends {} ms in the future, past the {} ms skew bound",
                obs.reporter.node_id,
                obs.window_end_unix_ms - t,
                snapshot.clock_skew_bound_ms
            ));
        }
        let key = (
            obs.partition.clone(),
            obs.topology_generation,
            obs.leaf.clone(),
        );
        if !covered.contains(&key) {
            continue;
        }
        if obs.window_start_unix_ms != w0 || obs.window_end_unix_ms != w1 {
            continue;
        }
        reports
            .entry(key)
            .or_insert_with(|| FragmentReports {
                current: BTreeMap::new(),
            })
            .current
            .insert((obs.shard.shard.clone(), obs.reporter.node_id.clone()), obs);
    }

    let policy_fingerprint = snapshot.policy.fingerprint();
    let plan_digest = snapshot.plan_digest();
    let mut placements = Vec::new();
    let mut moves: Vec<TierMove> = Vec::new();
    let mut refusals = Vec::new();
    let mut projected_free: BTreeMap<String, u64> = snapshot
        .nodes
        .iter()
        .map(|(node, record)| (node.clone(), record.capacity.free_bytes()))
        .collect();
    let mut planned_onto: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    'fragments: for fragment in &snapshot.fragments {
        let empty = FragmentReports {
            current: BTreeMap::new(),
        };
        let frag_reports = reports
            .get(&(
                fragment.partition.clone(),
                fragment.topology_generation,
                fragment.leaf.clone(),
            ))
            .unwrap_or(&empty);

        // Coverage: every shard's committed owner must have a current
        // report (unknown is not idle, E3), and so must every complete
        // copy (§3's freshness rule).
        let mut required: Vec<(&str, &str)> = fragment
            .shard_owners
            .iter()
            .map(|(shard, owner)| (shard.as_str(), owner.as_str()))
            .collect();
        for copy in fragment.complete_copies() {
            for shard in fragment.shard_owners.keys() {
                required.push((shard.as_str(), copy.node_id.as_str()));
            }
        }
        required.sort();
        required.dedup();
        for (shard, node) in required {
            match frag_reports
                .current
                .get(&(shard.to_string(), node.to_string()))
            {
                None => {
                    return Err(format!(
                        "plan_tiers: partition {} on node {} has no current observation; unknown is not idle",
                        fragment.partition.label(),
                        node
                    ));
                }
                Some(obs) => {
                    let age = t.saturating_sub(obs.window_end_unix_ms);
                    if age > snapshot.max_observation_age_ms {
                        return Err(format!(
                            "plan_tiers: partition {} on node {} was last observed {} ms ago, past the {} ms bound",
                            fragment.partition.label(),
                            node,
                            age,
                            snapshot.max_observation_age_ms
                        ));
                    }
                }
            }
        }

        // Aggregate (§3): owner-only bytes, scans over every reporter,
        // warmth as the maximum, checked fan-in.
        ensure_fan_in(frag_reports.current.len())?;
        let mut rows: u128 = 0;
        let mut bytes: u128 = 0;
        for (shard, owner) in &fragment.shard_owners {
            let obs = frag_reports
                .current
                .get(&(shard.clone(), owner.clone()))
                .expect("owner coverage checked above");
            rows += obs.rows as u128;
            bytes += obs.resident_bytes as u128;
        }
        let mut scans: u128 = 0;
        let mut warmth: u64 = 0;
        let mut figures = Vec::new();
        for ((shard, node), obs) in &frag_reports.current {
            scans += obs.scans_observed as u128;
            warmth = warmth.max(obs.last_scanned_unix_ms);
            figures.push(ReporterFigures {
                node_id: node.clone(),
                shard: shard.clone(),
                scans_observed: obs.scans_observed,
                queue_wait_p50_us: (obs.samples > 0).then_some(obs.queue_wait_p50_us),
                queue_wait_p99_us: (obs.samples > 0).then_some(obs.queue_wait_p99_us),
                samples: obs.samples,
                last_scanned_unix_ms: obs.last_scanned_unix_ms,
            });
        }
        if bytes == 0 {
            return Err(format!(
                "plan_tiers: fragment {} has 0 resident bytes from its committed owners",
                fragment.label()
            ));
        }
        let rate = rate_nanos(scans, snapshot.cohort_length_ms, bytes);

        // Classify (§5): first tier in precedence order whose band and
        // warmth bound the fragment satisfies.
        let mut classified: Option<&CapacityTier> = None;
        let mut unclassified_reason = None;
        'tiers: for tier in &snapshot.policy.tiers {
            if rate < tier.scans_per_byte_nanos_lo || rate >= tier.scans_per_byte_nanos_hi {
                continue;
            }
            if tier.max_seconds_since_scan > 0 {
                let bound_ms = tier
                    .max_seconds_since_scan
                    .checked_mul(1000)
                    .ok_or_else(|| {
                        format!(
                            "plan_tiers: tier {:?} warmth bound {} s overflows milliseconds",
                            tier.name, tier.max_seconds_since_scan
                        )
                    })?;
                if warmth == 0 {
                    unclassified_reason = Some(format!(
                        "tier {:?} needs a warmth bound of {} s and the fragment was never scanned",
                        tier.name, tier.max_seconds_since_scan
                    ));
                    continue 'tiers;
                }
                let age = t.saturating_sub(warmth);
                if age > bound_ms {
                    unclassified_reason = Some(format!(
                        "tier {:?} needs a scan within {} s; the last was {} ms ago",
                        tier.name, tier.max_seconds_since_scan, age
                    ));
                    continue 'tiers;
                }
            }
            classified = Some(tier);
            break;
        }
        let tier_name = classified.map(|tier| tier.name.clone());
        if tier_name.is_none() && unclassified_reason.is_none() {
            unclassified_reason = Some(format!("rate {rate} nanos is outside every band"));
        }

        let complete_replicas: Vec<CommittedCopy> = fragment.complete_copies().cloned().collect();
        placements.push(FragmentPlacement {
            fragment: fragment.clone(),
            classified_tier: tier_name.clone(),
            unclassified_reason,
            rate_nanos: rate,
            rows: u64::try_from(rows).map_err(|_| {
                format!(
                    "plan_tiers: fragment {} row count overflows u64",
                    fragment.label()
                )
            })?,
            resident_bytes: u64::try_from(bytes).map_err(|_| {
                format!(
                    "plan_tiers: fragment {} byte count overflows u64",
                    fragment.label()
                )
            })?,
            last_scanned_unix_ms: warmth,
            complete_replicas: complete_replicas.clone(),
            reporters: figures,
        });

        let Some(tier) = classified else {
            continue 'fragments;
        };

        // Violations (§4 step 2): floor first, then residency. The floor
        // counts complete copies in distinct failure domains (§2). The
        // deficit is a count with one source, never a cloned vector of
        // work orders.
        let mut seen_domains = std::collections::BTreeSet::new();
        let effective = fragment
            .complete_copies()
            .filter(|copy| seen_domains.insert(copy.failure_domain.as_str()))
            .count() as u32;
        let mut violation_sources: Vec<CommittedCopy> = Vec::new();
        if effective < tier.min_replicas {
            if fragment.complete_copies().next().is_none() {
                refusals.push(format!(
                    "plan_tiers: fragment {} has 0 complete replicas ({} partial); tier {:?} floor is {}",
                    fragment.label(),
                    fragment.copies.iter().filter(|c| !c.complete).count(),
                    tier.name,
                    tier.min_replicas
                ));
                continue 'fragments;
            }
            let source = fragment
                .complete_copies()
                .next()
                .expect("a complete copy exists")
                .clone();
            for _ in effective..tier.min_replicas {
                violation_sources.push(source.clone());
            }
        }
        let mut out_of_pool: Vec<CommittedCopy> = fragment
            .complete_copies()
            .filter(|copy| !fragment.pool.contains(&copy.node_id))
            .cloned()
            .collect();
        out_of_pool.sort_by(|a, b| {
            (&a.node_id, &a.storage_incarnation).cmp(&(&b.node_id, &b.storage_incarnation))
        });
        violation_sources.extend(out_of_pool);

        // Repair (§4 steps 3-5): candidates inside the pool, server-
        // resident and eligible per the committed node record, domain-
        // distinct at every intermediate step (§7), capacity-checked
        // against projected free bytes.
        let mut held_nodes: std::collections::BTreeSet<String> = fragment
            .complete_copies()
            .map(|copy| copy.node_id.clone())
            .collect();
        let mut held_domains: std::collections::BTreeSet<String> = fragment
            .complete_copies()
            .map(|copy| copy.failure_domain.clone())
            .collect();
        let fragment_bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        for source in violation_sources {
            if moves.len() as u64 >= snapshot.max_moves {
                refusals.push(format!(
                    "plan_tiers: max_moves {} reached; a violation of fragment {} is left unplanned",
                    snapshot.max_moves,
                    fragment.label()
                ));
                continue 'fragments;
            }
            let mut candidates: Vec<&String> = fragment
                .pool
                .iter()
                .filter(|node| !held_nodes.contains(node.as_str()))
                .filter(|node| {
                    snapshot
                        .nodes
                        .get(node.as_str())
                        .map(|record| {
                            record.residency == NodeResidency::Server
                                && record.eligible
                                && !held_domains.contains(&record.failure_domain)
                        })
                        .unwrap_or(false)
                })
                .collect();
            candidates.sort_by(|a, b| {
                let free_a = projected_free.get(a.as_str()).copied().unwrap_or(0);
                let free_b = projected_free.get(b.as_str()).copied().unwrap_or(0);
                free_b.cmp(&free_a).then_with(|| a.cmp(b))
            });
            let destination = candidates
                .iter()
                .find(|node| {
                    projected_free.get(node.as_str()).copied().unwrap_or(0) >= fragment_bytes
                })
                .map(|node| (*node).clone());
            let Some(destination) = destination else {
                let mut detail_nodes: Vec<&String> = candidates.clone();
                detail_nodes.sort();
                let mut detail = String::new();
                for (i, node) in detail_nodes.iter().enumerate() {
                    let free = projected_free.get(node.as_str()).copied().unwrap_or(0);
                    if i > 0 {
                        detail.push_str(", ");
                    }
                    if planned_onto.contains(node.as_str()) {
                        detail.push_str(&format!(
                            "{node} has {free} free after earlier planned moves"
                        ));
                    } else {
                        detail.push_str(&format!("{node} has {free} free"));
                    }
                }
                refusals.push(format!(
                    "plan_tiers: tier {:?} floor is {} for partition {}, but no eligible destination in leaf {} has capacity: {}, the fragment needs {}",
                    tier.name,
                    tier.min_replicas,
                    fragment.partition.label(),
                    fragment.leaf,
                    detail,
                    fragment_bytes
                ));
                continue;
            };
            *projected_free.entry(destination.clone()).or_insert(0) -= fragment_bytes;
            planned_onto.insert(destination.clone());
            held_nodes.insert(destination.clone());
            if let Some(record) = snapshot.nodes.get(&destination) {
                held_domains.insert(record.failure_domain.clone());
            }
            moves.push(TierMove {
                fragment: fragment.clone(),
                source: source.clone(),
                destination_node: destination,
                from_tier: tier.name.clone(),
                to_tier: tier.name.clone(),
                bytes: fragment_bytes,
                plan_digest,
                control_revision: snapshot.control_revision,
                observation_epoch: snapshot.observation_epoch,
                policy_fingerprint,
            });
        }
    }

    Ok(PlanTiersResponse {
        plan_digest,
        policy_fingerprint,
        observation_epoch: snapshot.observation_epoch,
        control_revision: snapshot.control_revision,
        placements,
        moves,
        refusals,
    })
}
