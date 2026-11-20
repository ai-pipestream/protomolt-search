//! The balance dry run: which moves would even out measured load.

use std::collections::{BTreeMap, BTreeSet};

use tonic::Status;

use crate::pb::{
    BalanceExclusion, BalanceMove, NodeLoad, NodeResidency, PlanBalanceRequest, PlanBalanceResponse,
};

use super::state::{StoredNode, StoredNodeState, StoredRole, StoredState};
use super::DurableControlPlane;

/// Defaults of a [`PlanBalanceRequest`].
pub const BALANCE_DEFAULT_MIN_GAIN: f64 = 0.10;
pub const BALANCE_DEFAULT_MAX_MOVES: u32 = 8;
pub const BALANCE_DEFAULT_MAX_RATE_AGE_MS: u64 = 10 * 60 * 1000;

/// The nodes a shard's primary may move among: the placement leaf's
/// node set when the topology has a tree and the leaf names nodes,
/// otherwise every node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BalancePool {
    pub leaf: String,
    /// `None` is "any node"; `Some` restricts to these node ids.
    pub node_ids: Option<BTreeSet<String>>,
}

/// One node as the planner sees it.
#[derive(Debug, Clone)]
struct BalanceNode {
    node_id: String,
    failure_domain: String,
    rate: u64,
    bytes: u64,
    /// `(shard index, shard id, replica addr, bytes)` of the primaries here.
    shards: Vec<(u32, String, String, u64)>,
    exclusion: Option<&'static str>,
}

impl BalanceNode {
    fn eligible(&self) -> bool {
        self.exclusion.is_none()
    }

    fn seconds(&self) -> f64 {
        match self.rate {
            0 => 0.0,
            rate => self.bytes as f64 / rate as f64,
        }
    }
}

/// Strip a scheme so a route address and a node's advertised address
/// compare as `host:port`.
pub(super) fn bare_addr(addr: &str) -> &str {
    addr.trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
}

/// The request's knobs with their defaults applied.
struct BalanceLimits {
    min_gain: f64,
    max_moves: u32,
    max_rate_age_ms: u64,
}

fn balance_limits(request: &PlanBalanceRequest, row_bytes: u64) -> Result<BalanceLimits, Status> {
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
    Ok(BalanceLimits {
        min_gain: if request.min_gain == 0.0 {
            BALANCE_DEFAULT_MIN_GAIN
        } else {
            request.min_gain
        },
        max_moves: if request.max_moves == 0 {
            BALANCE_DEFAULT_MAX_MOVES
        } else {
            request.max_moves
        },
        max_rate_age_ms: if request.max_rate_age_ms == 0 {
            BALANCE_DEFAULT_MAX_RATE_AGE_MS
        } else {
            request.max_rate_age_ms
        },
    })
}

/// Why a node takes no part in the plan, if anything.
fn node_exclusion(node: &StoredNode, now: u64, max_rate_age_ms: u64) -> Option<&'static str> {
    let capacity = &node.capacity;
    if node.state == StoredNodeState::Expired || node.expires_unix_ms <= now {
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
    }
}

fn balance_nodes(
    state: &StoredState,
    now: u64,
    max_rate_age_ms: u64,
) -> BTreeMap<String, BalanceNode> {
    state
        .nodes
        .iter()
        .map(|(node_id, node)| {
            let capacity = &node.capacity;
            let exclusion = node_exclusion(node, now, max_rate_age_ms);
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
        .collect()
}

/// Place every primary on its node and return the ready replicas' failure
/// domains per shard, for the failure-domain rule: a primary is not moved
/// into the domain of a copy that would then share it.
fn assign_primaries(
    state: &StoredState,
    row_bytes: u64,
    shard_index: &BTreeMap<&str, u32>,
    nodes: &mut BTreeMap<String, BalanceNode>,
) -> BTreeMap<String, BTreeSet<String>> {
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
    replica_domains
}

fn slowest(nodes: &BTreeMap<String, BalanceNode>) -> f64 {
    nodes
        .values()
        .filter(|n| n.eligible())
        .map(BalanceNode::seconds)
        .fold(0.0, f64::max)
}

/// The slowest eligible node that holds a primary; ties by id.
fn slowest_source(nodes: &BTreeMap<String, BalanceNode>) -> Option<String> {
    nodes
        .values()
        .filter(|n| n.eligible() && !n.shards.is_empty())
        .max_by(|a, b| {
            a.seconds()
                .partial_cmp(&b.seconds())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.node_id.cmp(&a.node_id))
        })
        .map(|n| n.node_id.clone())
}

/// The slowest eligible node's seconds once `bytes` moves from `source_id`
/// to `target_id`.
fn seconds_after_move(
    nodes: &BTreeMap<String, BalanceNode>,
    source_id: &str,
    target_id: &str,
    bytes: u64,
) -> f64 {
    nodes
        .values()
        .filter(|n| n.eligible())
        .map(|n| {
            let bytes_after = if n.node_id == source_id {
                n.bytes - bytes
            } else if n.node_id == target_id {
                n.bytes + bytes
            } else {
                n.bytes
            };
            bytes_after as f64 / n.rate as f64
        })
        .fold(0.0, f64::max)
}

/// The inputs every move of one plan shares.
struct MoveContext<'a> {
    replica_domains: &'a BTreeMap<String, BTreeSet<String>>,
    pool_of: &'a dyn Fn(&str) -> BalancePool,
}

/// The best single move off `source`: `(seconds after, target node, position
/// of the shard in the source's list)`.
fn best_move(
    nodes: &BTreeMap<String, BalanceNode>,
    source: &BalanceNode,
    context: &MoveContext<'_>,
) -> Option<(f64, String, usize)> {
    let source_id = source.node_id.as_str();
    let mut best: Option<(f64, String, usize)> = None;
    for (position, (_, shard_id, addr, bytes)) in source.shards.iter().enumerate() {
        let pool = (context.pool_of)(addr);
        let domains = context.replica_domains.get(shard_id);
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
            let after = seconds_after_move(nodes, source_id, &target.node_id, *bytes);
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
    best
}

/// Greedily move primaries off the slowest node while each move gains at
/// least `min_gain`; returns the moves and the slowest node's final seconds.
fn plan_moves(
    nodes: &mut BTreeMap<String, BalanceNode>,
    context: &MoveContext<'_>,
    limits: &BalanceLimits,
    seconds_before: f64,
) -> (Vec<BalanceMove>, f64) {
    let mut moves = Vec::new();
    let mut current = seconds_before;
    for _ in 0..limits.max_moves {
        let Some(source_id) = slowest_source(nodes) else {
            break;
        };
        let source = nodes[&source_id].clone();
        let Some((after, target_id, position)) = best_move(nodes, &source, context) else {
            break;
        };
        if after > current * (1.0 - limits.min_gain) {
            break;
        }
        let (index, shard_id, addr, bytes) = source.shards[position].clone();
        let leaf = (context.pool_of)(&addr).leaf;
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
    (moves, current)
}

/// Per-node loads as of the state BEFORE the moves: what the plan saw.
fn node_loads(
    state: &StoredState,
    nodes: &BTreeMap<String, BalanceNode>,
    shard_index: &BTreeMap<&str, u32>,
    row_bytes: u64,
) -> Vec<NodeLoad> {
    state
        .nodes
        .keys()
        .map(|node_id| {
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
        .collect()
}

/// The balance dry run: a pure function of the durable state, the request,
/// the provider's encoded row bytes, the per-shard pools, and the clock. It
/// moves nothing.
fn plan_balance(
    state: &StoredState,
    request: &PlanBalanceRequest,
    row_bytes: u64,
    pool_of: &dyn Fn(&str) -> BalancePool,
    now: u64,
) -> Result<PlanBalanceResponse, Status> {
    let limits = balance_limits(request, row_bytes)?;
    let shard_index: BTreeMap<&str, u32> = state
        .topology
        .routes
        .iter()
        .enumerate()
        .map(|(i, route)| (bare_addr(&route.addr), i as u32))
        .collect();
    let mut nodes = balance_nodes(state, now, limits.max_rate_age_ms);
    let replica_domains = assign_primaries(state, row_bytes, &shard_index, &mut nodes);
    let seconds_before = slowest(&nodes);
    let context = MoveContext {
        replica_domains: &replica_domains,
        pool_of,
    };
    let (moves, current) = plan_moves(&mut nodes, &context, &limits, seconds_before);
    let loads = node_loads(state, &nodes, &shard_index, row_bytes);
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
        min_gain: limits.min_gain,
        max_moves: limits.max_moves,
    })
}

impl DurableControlPlane {
    /// The balance dry run over the durable state. It moves nothing.
    pub fn plan_balance(
        &self,
        request: &PlanBalanceRequest,
        row_bytes: u64,
        pool_of: &dyn Fn(&str) -> BalancePool,
        now: u64,
    ) -> Result<PlanBalanceResponse, Status> {
        let state = self.lock_state()?;
        plan_balance(&state, request, row_bytes, pool_of, now)
    }
}
