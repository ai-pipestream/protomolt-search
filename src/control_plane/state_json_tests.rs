use super::*;

/// A complete nondefault state.
fn complete_state() -> StoredState {
    let capacity = StoredCapacity {
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
    let node_a = StoredNode {
        node_id: "node/α".into(),
        addr: "http://node-a:9000".into(),
        state: StoredNodeState::Active,
        lease_token: u64::MAX - 2,
        expires_unix_ms: (1u64 << 53) + 900_000,
        capacity: capacity.clone(),
    };
    let node_b = StoredNode {
        node_id: "node z".into(),
        addr: "https://node-b:9443/".into(),
        state: StoredNodeState::Draining,
        lease_token: u64::MAX - 1,
        expires_unix_ms: (1u64 << 53) + 900_001,
        capacity: StoredCapacity {
            residency: NodeResidency::Device as i32,
            ..capacity
        },
    };
    let replica_a = StoredReplica {
        shard_id: "shard-a".into(),
        node_id: node_a.node_id.clone(),
        addr: node_a.addr.clone(),
        generation: 7,
        hash_lo: 0,
        hash_hi: 99,
        slot_offset: (1u64 << 53) + 11,
        rows: (1u64 << 53) + 101,
        bytes: (1u64 << 53) + 2_020,
        role: StoredRole::Primary,
        ready: true,
        scoring_fingerprint: "score-a".into(),
        analysis_fingerprint: "analysis-a".into(),
        immutable_segments: 4,
        tombstones: (1u64 << 53) + 5,
    };
    let replica_b = StoredReplica {
        shard_id: "shard-a".into(),
        node_id: node_b.node_id.clone(),
        addr: node_b.addr.clone(),
        generation: 8,
        hash_lo: 0,
        hash_hi: 99,
        slot_offset: (1u64 << 53) + 12,
        rows: (1u64 << 53) + 100,
        bytes: (1u64 << 53) + 2_000,
        role: StoredRole::Replica,
        ready: false,
        scoring_fingerprint: "score-b".into(),
        analysis_fingerprint: "analysis-b".into(),
        immutable_segments: 2,
        tombstones: (1u64 << 53) + 3,
    };
    let route = |addr: &str, replica: Option<&str>, lo, hi| StoredRoute {
        addr: addr.into(),
        replica: replica.map(str::to_owned),
        hash_lo: lo,
        hash_hi: hi,
    };
    StoredState {
        format: 1,
        collection: "books/2026".into(),
        revision: u64::MAX,
        next_token: u64::MAX,
        next_action: u64::MAX,
        topology: StoredTopology {
            generation: 3,
            routes: vec![
                route("http://node-a:9000", Some("replica-a"), 0, 99),
                route("https://node-b:9443/", None, 100, u64::MAX),
            ],
        },
        history: vec![
            StoredTopology {
                generation: 1,
                routes: vec![route("old-a", None, 0, u64::MAX)],
            },
            StoredTopology {
                generation: 2,
                routes: vec![route("old-b", Some(""), 0, u64::MAX)],
            },
        ],
        nodes: [
            (node_a.node_id.clone(), node_a),
            (node_b.node_id.clone(), node_b),
        ]
        .into_iter()
        .collect(),
        replicas: [
            (
                format!("{}\0{}", replica_a.shard_id, replica_a.node_id),
                replica_a,
            ),
            (
                format!("{}\0{}", replica_b.shard_id, replica_b.node_id),
                replica_b,
            ),
        ]
        .into_iter()
        .collect(),
        actions: vec![StoredAction {
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
        completed_actions: [3, 9].into_iter().collect(),
    }
}

#[test]
fn json_defaults_round_trip_and_unknown_nested_fields_refuse_without_rewrite() {
    let mut old_value = serde_json::to_value(complete_state()).unwrap();
    old_value.as_object_mut().unwrap().remove("collection");
    old_value
        .as_object_mut()
        .unwrap()
        .remove("completed_actions");
    for node in old_value["nodes"].as_object_mut().unwrap().values_mut() {
        let capacity = node["capacity"].as_object_mut().unwrap();
        for field in [
            "scan_bytes_per_second",
            "scan_rate_observed_unix_ms",
            "scan_rate_samples",
            "scan_rate_window_ms",
            "residency",
        ] {
            capacity.remove(field);
        }
    }
    old_value["actions"][0]
        .as_object_mut()
        .unwrap()
        .remove("peer_source_generation");
    let old: StoredState = serde_json::from_value(old_value).unwrap();
    assert_eq!(old.collection, "");
    assert!(old.completed_actions.is_empty());
    assert!(old.nodes.values().all(|node| {
        node.capacity.scan_bytes_per_second == 0
            && node.capacity.scan_rate_observed_unix_ms == 0
            && node.capacity.scan_rate_samples == 0
            && node.capacity.scan_rate_window_ms == 0
            && node.capacity.residency == 0
    }));
    assert_eq!(old.actions[0].peer_source_generation, 0);
    let restored: StoredState = serde_json::from_slice(&serde_json::to_vec(&old).unwrap()).unwrap();
    assert_eq!(
        serde_json::to_vec(&restored).unwrap(),
        serde_json::to_vec(&old).unwrap()
    );

    for level in [
        "state", "topology", "route", "node", "capacity", "replica", "action",
    ] {
        let mut value = serde_json::to_value(complete_state()).unwrap();
        let object = match level {
            "state" => value.as_object_mut().unwrap(),
            "topology" => value["topology"].as_object_mut().unwrap(),
            "route" => value["topology"]["routes"][0].as_object_mut().unwrap(),
            "node" => value["nodes"]
                .as_object_mut()
                .unwrap()
                .values_mut()
                .next()
                .unwrap()
                .as_object_mut()
                .unwrap(),
            "capacity" => value["nodes"]
                .as_object_mut()
                .unwrap()
                .values_mut()
                .next()
                .unwrap()["capacity"]
                .as_object_mut()
                .unwrap(),
            "replica" => value["replicas"]
                .as_object_mut()
                .unwrap()
                .values_mut()
                .next()
                .unwrap()
                .as_object_mut()
                .unwrap(),
            "action" => value["actions"][0].as_object_mut().unwrap(),
            _ => unreachable!(),
        };
        object.insert("future_decision".into(), serde_json::json!(7));
        let error = serde_json::from_value::<StoredState>(value).unwrap_err();
        assert!(
            error.to_string().contains("unknown field"),
            "{level}: {error}"
        );
    }
    let mut missing = serde_json::to_value(complete_state()).unwrap();
    missing.as_object_mut().unwrap().remove("revision");
    assert!(serde_json::from_value::<StoredState>(missing)
        .unwrap_err()
        .to_string()
        .contains("missing field"));

    let root = std::env::temp_dir().join(format!(
        "control-state-json-{}-{}",
        std::process::id(),
        now_ms()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("state.json");
    let plane = DurableControlPlane::open(&path, ControlPolicy::default()).unwrap();
    drop(plane);
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    value["topology"]["future_decision"] = serde_json::json!(7);
    let damaged = serde_json::to_vec(&value).unwrap();
    std::fs::write(&path, &damaged).unwrap();
    let error = DurableControlPlane::open_existing(&path, ControlPolicy::default())
        .err()
        .unwrap();
    assert!(error.contains("unknown field"), "{error}");
    assert_eq!(std::fs::read(&path).unwrap(), damaged);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn json_duplicate_maps_and_sets_are_refused_instead_of_normalized() {
    let state = complete_state();
    let mut node = state.nodes.values().next().unwrap().clone();
    node.node_id = "n".into();
    let node = serde_json::to_string(&node).unwrap();
    let mut replica = state.replicas.values().next().unwrap().clone();
    replica.shard_id = "s".into();
    replica.node_id = "n".into();
    let replica = serde_json::to_string(&replica).unwrap();
    let prefix = r#""format":1,"revision":1,"next_token":2,"next_action":10,"topology":{"generation":0,"routes":[]},"history":[]"#;
    let cases = [
        (
            "node",
            format!(
                "{{{prefix},\"nodes\":{{\"n\":{node},\"n\":{node}}},\"replicas\":{{}},\"actions\":[]}}"
            ),
        ),
        (
            "replica",
            format!(
                "{{{prefix},\"nodes\":{{}},\"replicas\":{{\"s\\u0000n\":{replica},\"s\\u0000n\":{replica}}},\"actions\":[]}}"
            ),
        ),
        (
            "completed action",
            format!(
                "{{{prefix},\"nodes\":{{}},\"replicas\":{{}},\"actions\":[],\"completed_actions\":[3,3]}}"
            ),
        ),
    ];
    for (name, json) in cases {
        let error = serde_json::from_str::<StoredState>(&json).unwrap_err();
        assert!(error.to_string().contains("duplicate"), "{name}: {error}");
    }
}
