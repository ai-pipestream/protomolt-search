//! The balance dry run: a pure function of the durable state and the
//! request (`docs/bandwidth-budget.md`).

use super::*;

/// The balance dry run (`docs/bandwidth-budget.md`): a pure function of
/// the durable state, the request, the provider's encoded row bytes, the
/// per-shard pools, and the clock. It moves nothing.
pub(super) fn plan_balance(
    state: &StoredState,
    request: &PlanBalanceRequest,
    row_bytes: u64,
    pool_of: &dyn Fn(&str) -> BalancePool,
    now: u64,
) -> Result<PlanBalanceResponse, Status> {
    if !request.min_gain.is_finite() || !(0.0..=1.0).contains(&request.min_gain) {
        return Err(Status::invalid_argument(format!(
            "plan_balance: min_gain {} is outside [0, 1] (0 selects {BALANCE_DEFAULT_MIN_GAIN})",
            request.min_gain
        )));
    }
    if row_bytes == 0 {
        return Err(Status::failed_precondition(
            "plan_balance: the provider's encoded row bytes are unknown (no reachable shard \
             reported its geometry), so no load can be expressed in the rate's units",
        ));
    }
    let min_gain = if request.min_gain == 0.0 {
        BALANCE_DEFAULT_MIN_GAIN
    } else {
        request.min_gain
    };
    let max_moves = if request.max_moves == 0 {
        BALANCE_DEFAULT_MAX_MOVES
    } else {
        request.max_moves
    };
    let max_rate_age_ms = if request.max_rate_age_ms == 0 {
        BALANCE_DEFAULT_MAX_RATE_AGE_MS
    } else {
        request.max_rate_age_ms
    };
    let shard_index: BTreeMap<&str, u32> = state
        .topology
        .routes
        .iter()
        .enumerate()
        .map(|(i, route)| (bare_addr(&route.addr), i as u32))
        .collect();

    let mut nodes: BTreeMap<String, BalanceNode> = state
        .nodes
        .iter()
        .map(|(node_id, node)| {
            let capacity = &node.capacity;
            let exclusion = if node.state == StoredNodeState::Expired || node.expires_unix_ms <= now
            {
                Some("no-lease")
            } else if node.state == StoredNodeState::Draining {
                Some("draining")
            } else if capacity.residency == NodeResidency::Device as i32 {
                Some("device")
            } else if capacity.residency != NodeResidency::Server as i32 {
                Some("residency-unspecified")
            } else if capacity.scan_bytes_per_second == 0 {
                Some("unmeasured")
            } else if now.saturating_sub(capacity.scan_rate_observed_unix_ms) > max_rate_age_ms {
                Some("stale")
            } else {
                None
            };
            (
                node_id.clone(),
                BalanceNode {
                    node_id: node_id.clone(),
                    failure_domain: capacity.failure_domain.clone(),
                    rate: if exclusion.is_none() {
                        capacity.scan_bytes_per_second
                    } else {
                        0
                    },
                    bytes: 0,
                    shards: Vec::new(),
                    exclusion,
                },
            )
        })
        .collect();
    // Ready replicas per shard, for the failure-domain rule: a primary
    // is not moved into the domain of a copy that would then share it.
    let mut replica_domains: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for replica in state.replicas.values() {
        if replica.role == StoredRole::Primary {
            let bytes = replica.rows.saturating_mul(row_bytes);
            if let Some(node) = nodes.get_mut(&replica.node_id) {
                let index = shard_index
                    .get(bare_addr(&replica.addr))
                    .copied()
                    .unwrap_or(u32::MAX);
                node.bytes = node.bytes.saturating_add(bytes);
                node.shards
                    .push((index, replica.shard_id.clone(), replica.addr.clone(), bytes));
            }
        } else if replica.ready {
            if let Some(node) = state.nodes.get(&replica.node_id) {
                replica_domains
                    .entry(replica.shard_id.clone())
                    .or_default()
                    .insert(node.capacity.failure_domain.clone());
            }
        }
    }
    for node in nodes.values_mut() {
        node.shards.sort_by(|a, b| a.1.cmp(&b.1));
    }

    let slowest = |nodes: &BTreeMap<String, BalanceNode>| -> f64 {
        nodes
            .values()
            .filter(|n| n.eligible())
            .map(BalanceNode::seconds)
            .fold(0.0, f64::max)
    };
    let seconds_before = slowest(&nodes);
    let mut moves = Vec::new();
    let mut current = seconds_before;
    for _ in 0..max_moves {
        // The slowest eligible node that holds a primary; ties by id.
        let Some(source_id) = nodes
            .values()
            .filter(|n| n.eligible() && !n.shards.is_empty())
            .max_by(|a, b| {
                a.seconds()
                    .partial_cmp(&b.seconds())
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.node_id.cmp(&a.node_id))
            })
            .map(|n| n.node_id.clone())
        else {
            break;
        };
        let source = nodes[&source_id].clone();
        let mut best: Option<(f64, String, usize)> = None;
        for (position, (_, shard_id, addr, bytes)) in source.shards.iter().enumerate() {
            let pool = pool_of(addr);
            let domains = replica_domains.get(shard_id);
            for target in nodes.values() {
                if !target.eligible() || target.node_id == source_id {
                    continue;
                }
                if let Some(allowed) = &pool.node_ids {
                    if !allowed.contains(&target.node_id) {
                        continue;
                    }
                }
                if domains.is_some_and(|d| d.contains(&target.failure_domain)) {
                    continue;
                }
                let after = nodes
                    .values()
                    .filter(|n| n.eligible())
                    .map(|n| {
                        let bytes_after = if n.node_id == source_id {
                            n.bytes - bytes
                        } else if n.node_id == target.node_id {
                            n.bytes + bytes
                        } else {
                            n.bytes
                        };
                        bytes_after as f64 / n.rate as f64
                    })
                    .fold(0.0, f64::max);
                let better = match &best {
                    None => true,
                    Some((best_after, best_target, best_position)) => {
                        after < *best_after
                            || (after == *best_after
                                && (target.node_id.as_str(), shard_id.as_str())
                                    < (
                                        best_target.as_str(),
                                        source.shards[*best_position].1.as_str(),
                                    ))
                    }
                };
                if better {
                    best = Some((after, target.node_id.clone(), position));
                }
            }
        }
        let Some((after, target_id, position)) = best else {
            break;
        };
        if after > current * (1.0 - min_gain) {
            break;
        }
        let (index, shard_id, addr, bytes) = source.shards[position].clone();
        let leaf = pool_of(&addr).leaf;
        {
            let from = nodes.get_mut(&source_id).expect("source exists");
            from.bytes -= bytes;
            from.shards.remove(position);
        }
        {
            let to = nodes.get_mut(&target_id).expect("target exists");
            to.bytes += bytes;
            to.shards.push((index, shard_id.clone(), addr, bytes));
            to.shards.sort_by(|a, b| a.1.cmp(&b.1));
        }
        current = after;
        moves.push(BalanceMove {
            shard: index,
            from_node: source_id,
            to_node: target_id,
            bytes,
            leaf,
            seconds_after: after,
        });
    }

    let loads = state
        .nodes
        .keys()
        .map(|node_id| {
            // Loads report the state BEFORE the moves: what the plan saw.
            let bytes: u64 = state
                .replicas
                .values()
                .filter(|r| r.role == StoredRole::Primary && &r.node_id == node_id)
                .map(|r| r.rows.saturating_mul(row_bytes))
                .fold(0u64, u64::saturating_add);
            let node = &state.nodes[node_id];
            let measured = nodes[node_id].eligible();
            let mut shards: Vec<u32> = state
                .replicas
                .values()
                .filter(|r| r.role == StoredRole::Primary && &r.node_id == node_id)
                .map(|r| {
                    shard_index
                        .get(bare_addr(&r.addr))
                        .copied()
                        .unwrap_or(u32::MAX)
                })
                .collect();
            shards.sort_unstable();
            NodeLoad {
                node_id: node_id.clone(),
                bytes,
                scan_bytes_per_second: node.capacity.scan_bytes_per_second,
                seconds: if measured && node.capacity.scan_bytes_per_second > 0 {
                    bytes as f64 / node.capacity.scan_bytes_per_second as f64
                } else {
                    0.0
                },
                shards,
                residency: node.capacity.residency,
            }
        })
        .collect();
    let excluded = nodes
        .values()
        .filter_map(|n| {
            n.exclusion.map(|reason| BalanceExclusion {
                node_id: n.node_id.clone(),
                reason: reason.to_string(),
            })
        })
        .collect();
    Ok(PlanBalanceResponse {
        topology_generation: state.topology.generation,
        control_revision: state.revision,
        loads,
        moves,
        seconds_before,
        seconds_after: current,
        excluded,
        min_gain,
        max_moves,
    })
}
