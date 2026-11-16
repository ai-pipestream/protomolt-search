//! The control state every install test starts from.
use super::*;

pub(super) fn route(addr: &str, replica: Option<&str>, hash_lo: u64, hash_hi: u64) -> ControlRoute {
    ControlRoute {
        addr: addr.into(),
        replica: replica.map(str::to_owned),
        hash_lo,
        hash_hi,
    }
}

/// node/α on a server and node z on a device, sharing every other
/// capacity fact.
fn nodes() -> (ControlNodeRecord, ControlNodeRecord) {
    let capacity = NodeCapacity {
        disk_bytes: (1u64 << 53) + 10_000,
        used_disk_bytes: (1u64 << 53) + 4_321,
        memory_bytes: (1u64 << 53) + 8_192,
        search_threads: 3,
        failure_domain: "rack/ß".into(),
        scan_bytes_per_second: (1u64 << 53) + 987,
        scan_rate_observed_unix_ms: (1u64 << 53) + 123_456,
        scan_rate_samples: 17,
        scan_rate_window_ms: 2_000,
        residency: NodeResidency::Server as i32,
    };
    let node_a = ControlNodeRecord {
        node_id: "node/α".into(),
        addr: "http://node-a:9000".into(),
        state: ClusterNodeState::Active as i32,
        lease_token: u64::MAX - 2,
        expires_unix_ms: (1u64 << 53) + 900_000,
        capacity: Some(capacity.clone()),
    };
    let node_b = ControlNodeRecord {
        node_id: "node z".into(),
        addr: "https://node-b:9443/".into(),
        state: ClusterNodeState::Draining as i32,
        lease_token: u64::MAX - 1,
        expires_unix_ms: (1u64 << 53) + 900_001,
        capacity: Some(NodeCapacity {
            residency: NodeResidency::Device as i32,
            ..capacity
        }),
    };
    (node_a, node_b)
}

/// shard-a on both nodes: the primary on node/α at generation 7 and a
/// replica on node z.
fn replicas(
    node_a: &ControlNodeRecord,
    node_b: &ControlNodeRecord,
) -> (ControlReplicaRecord, ControlReplicaRecord) {
    let replica_a = ControlReplicaRecord {
        shard_id: "shard-a".into(),
        node_id: node_a.node_id.clone(),
        addr: node_a.addr.clone(),
        generation: 7,
        hash_lo: 0,
        hash_hi: 99,
        slot_offset: (1u64 << 53) + 11,
        rows: (1u64 << 53) + 101,
        bytes: (1u64 << 53) + 2_020,
        role: ShardReplicaRole::Primary as i32,
        ready: true,
        scoring_fingerprint: "score-a".into(),
        analysis_fingerprint: "analysis-a".into(),
        immutable_segments: 4,
        tombstones: (1u64 << 53) + 5,
    };
    let replica_b = ControlReplicaRecord {
        shard_id: "shard-a".into(),
        node_id: node_b.node_id.clone(),
        addr: node_b.addr.clone(),
        generation: 8,
        hash_lo: 0,
        hash_hi: 99,
        slot_offset: (1u64 << 53) + 12,
        rows: (1u64 << 53) + 100,
        bytes: (1u64 << 53) + 2_000,
        role: ShardReplicaRole::Replica as i32,
        ready: false,
        scoring_fingerprint: "score-b".into(),
        analysis_fingerprint: "analysis-b".into(),
        immutable_segments: 2,
        tombstones: (1u64 << 53) + 3,
    };
    (replica_a, replica_b)
}

/// `routes` current routes: the two fixture routes, then three-hash-wide
/// extras from hash 100.
fn current_routes(routes: usize) -> Vec<ControlRoute> {
    let mut current = vec![
        route("http://node-a:9000", Some("replica-a"), 0, 99),
        route("https://node-b:9443/", None, 100, u64::MAX),
    ];
    for extra in 2..routes {
        let lo = 100 + (extra as u64 - 2) * 3;
        current.push(route(
            &format!("https://node-{extra:04}.example:9443/shard"),
            Some(&format!("replica-{extra:04}")),
            lo,
            lo + 2,
        ));
    }
    current
}

/// Two nodes (node/α server, node z device), shard-a on both (routes
/// 0..=99, primary node/α at generation 7), one pending copy, completed ids
/// 3 and 9, and generation 3 with `routes` current routes (at least 2)
/// after history generations 1 and 2.
pub(crate) fn state(routes: usize) -> ControlInstallState {
    let (node_a, node_b) = nodes();
    let (replica_a, replica_b) = replicas(&node_a, &node_b);
    let current = current_routes(routes);
    ControlInstallState {
        next_token: u64::MAX,
        next_action: u64::MAX,
        topology: Some(ControlRouteTable {
            generation: 3,
            routes: current,
        }),
        history: vec![
            ControlRouteTable {
                generation: 1,
                routes: vec![route("old-a", None, 0, u64::MAX)],
            },
            ControlRouteTable {
                generation: 2,
                routes: vec![route("old-b", Some(""), 0, u64::MAX)],
            },
        ],
        // node_id order: "node z" < "node/α".
        nodes: vec![node_b, node_a],
        replicas: vec![replica_b, replica_a],
        actions: vec![ControlActionRecord {
            action_id: u64::MAX - 1,
            kind: PlacementActionKind::CopyReplica as i32,
            shard_id: "shard-a".into(),
            peer_shard_id: "peer-a".into(),
            peer_source_generation: 6,
            source_node_id: "node/α".into(),
            target_node_id: "node z".into(),
            source_generation: 7,
            target_generation: 8,
            hash_lo: 0,
            hash_hi: 99,
            reason: "rebalance exactly".into(),
        }],
        completed_actions: vec![3, 9],
        configuration: Some(configuration()),
    }
}

pub(crate) fn tree() -> PlacementTree {
    PlacementTree {
        column: "placement".into(),
        level_bits: 0,
        nodes: vec![
            PlacementNode {
                name: "old".into(),
                cel: "year < 2020".into(),
                ..Default::default()
            },
            PlacementNode {
                name: "rest".into(),
                ..Default::default()
            },
        ],
    }
}

/// The two-route fixture state with a placement tree, so shard-a (routes
/// 0..99, primary node/α, generation 7) sits in leaf "old".
pub(crate) fn state_with_tree() -> ControlInstallState {
    let placement = crate::placement::Placement::validate(
        &crate::placement::PlacementTreeConfig::from_proto(&tree()),
    )
    .unwrap();
    let mut state = state(2);
    let configuration = state.configuration.as_mut().unwrap();
    configuration.placement = Some(Placement::Tree(tree()));
    configuration.route_codes = ["old", "rest"]
        .map(|name| PlacementRouteCode {
            has_placement: true,
            placement: placement.leaf_by_name(name).unwrap().code as u64,
        })
        .to_vec();
    state
}
