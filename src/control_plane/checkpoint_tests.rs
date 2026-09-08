use super::*;
use crate::pb::storage::*;
use prost::Message;

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

fn complete_policy() -> ControlPolicy {
    ControlPolicy {
        lease_ms: 31_337,
        replication_factor: 3,
        split_rows: 123_456,
        merge_rows: 7_654,
        compact_segments: 11,
        compact_tombstone_ppm: 222_333,
        history_limit: 17,
    }
}

#[test]
fn complete_nondefault_state_round_trips_without_losing_order_or_presence() {
    let state = complete_state();
    let policy = complete_policy();
    let expected_json = serde_json::to_vec(&state).unwrap();
    let bytes = checkpoint::encode(&state, &policy).unwrap();
    let decoded = LegacyControlCheckpoint::decode(&bytes).unwrap();
    assert_eq!(decoded.encode(), bytes);
    assert_eq!(
        serde_json::to_vec(&checkpoint::restore(decoded.state()).unwrap()).unwrap(),
        expected_json
    );

    let captured_policy = decoded.policy();
    assert_eq!(captured_policy.lease_ms, policy.lease_ms);
    assert_eq!(
        captured_policy.replication_factor,
        policy.replication_factor as u64
    );
    assert_eq!(captured_policy.split_rows, policy.split_rows);
    assert_eq!(captured_policy.merge_rows, policy.merge_rows);
    assert_eq!(captured_policy.compact_segments, policy.compact_segments);
    assert_eq!(
        captured_policy.compact_tombstone_ppm,
        policy.compact_tombstone_ppm
    );
    assert_eq!(captured_policy.history_limit, policy.history_limit as u64);
    assert_eq!(decoded.state().history[0].generation, 1);
    assert_eq!(decoded.state().history[1].generation, 2);
    assert_eq!(decoded.state().history[0].routes[0].replica, None);
    assert_eq!(
        decoded.state().history[1].routes[0].replica,
        Some(String::new())
    );
    assert_eq!(decoded.state().completed_actions, vec![3, 9]);
}

#[test]
fn minimal_default_state_has_a_stable_import_wire_layout() {
    let policy = ControlPolicy {
        lease_ms: 0,
        replication_factor: 0,
        split_rows: 0,
        merge_rows: 0,
        compact_segments: 0,
        compact_tombstone_ppm: 0,
        history_limit: 0,
    };
    let expected = b"\x08\x01\x12\x0a\x08\x01\x18\x01\x20\x01\x28\x01\x32\x00\x1a\x00";
    assert_eq!(
        checkpoint::encode(&StoredState::default(), &policy).unwrap(),
        expected
    );
    let decoded = LegacyControlCheckpoint::decode(expected).unwrap();
    assert_eq!(decoded.encode(), expected);
    assert_eq!(
        serde_json::to_vec(&checkpoint::restore(decoded.state()).unwrap()).unwrap(),
        serde_json::to_vec(&StoredState::default()).unwrap()
    );
    assert_eq!(decoded.policy(), &LegacyControlPolicy::default());
}

fn valid_import() -> LegacyControlImport {
    LegacyControlImport::decode(
        checkpoint::encode(&complete_state(), &complete_policy())
            .unwrap()
            .as_slice(),
    )
    .unwrap()
}

#[test]
fn decode_refuses_noncanonical_or_structurally_ambiguous_control_state() {
    let valid = valid_import();
    let mut cases: Vec<(&str, LegacyControlImport, &str)> = Vec::new();

    let mut value = valid.clone();
    value.format_version = 2;
    cases.push(("outer format", value, "format 1"));
    let mut value = valid.clone();
    value.state = None;
    cases.push(("missing state", value, "state is required"));
    let mut value = valid.clone();
    value.policy = None;
    cases.push(("missing policy", value, "policy is required"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().topology = None;
    cases.push(("missing topology", value, "topology is required"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().nodes[0].node = None;
    cases.push(("missing node", value, "missing node"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().nodes[0]
        .node
        .as_mut()
        .unwrap()
        .capacity = None;
    cases.push(("missing capacity", value, "capacity is required"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().replicas[0].replica = None;
    cases.push(("missing replica", value, "missing replica"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().revision = 0;
    cases.push(("zero revision", value, "nonzero revision"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().next_token = u64::MAX - 1;
    cases.push(("lease allocator", value, "lease token"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().next_action = u64::MAX - 1;
    cases.push(("action allocator", value, "pending action ID"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().nodes[0]
        .node
        .as_mut()
        .unwrap()
        .state = 99;
    cases.push(("unknown node state", value, "node state"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().replicas[0]
        .replica
        .as_mut()
        .unwrap()
        .role = 99;
    cases.push(("unknown replica role", value, "replica role"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().actions[0].kind = 99;
    cases.push(("unknown action kind", value, "action kind"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().nodes[0]
        .node
        .as_mut()
        .unwrap()
        .capacity
        .as_mut()
        .unwrap()
        .residency = 99;
    cases.push(("unknown residency", value, "node residency"));
    let mut value = valid.clone();
    let token = value.state.as_ref().unwrap().nodes[0]
        .node
        .as_ref()
        .unwrap()
        .lease_token;
    value.state.as_mut().unwrap().nodes[1]
        .node
        .as_mut()
        .unwrap()
        .lease_token = token;
    cases.push(("duplicate lease token", value, "lease token"));
    let mut value = valid.clone();
    value
        .state
        .as_mut()
        .unwrap()
        .completed_actions
        .push(u64::MAX - 1);
    cases.push((
        "completed and pending action overlap",
        value,
        "pending action ID",
    ));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().nodes[0].key = "node y".into();
    cases.push(("node key mismatch", value, "node map key"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().replicas[0].key = "shard-a\0node y".into();
    cases.push(("replica key mismatch", value, "replica map key"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().nodes.swap(0, 1);
    cases.push(("unsorted node keys", value, "unique and sorted"));
    let mut value = valid.clone();
    let duplicate = value.state.as_ref().unwrap().nodes[0].clone();
    value.state.as_mut().unwrap().nodes.push(duplicate);
    cases.push(("duplicate node key", value, "unique and sorted"));
    let mut value = valid.clone();
    value.state.as_mut().unwrap().completed_actions = vec![9, 3];
    cases.push(("unsorted completed actions", value, "sorted unique"));
    let mut value = valid;
    value.state.as_mut().unwrap().completed_actions = vec![3, 3];
    cases.push(("duplicate completed action", value, "sorted unique"));

    for (name, value, reason) in cases {
        let error = LegacyControlCheckpoint::decode(&value.encode_to_vec())
            .err()
            .unwrap();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{name}: {error}"
        );
        assert!(error.message().contains(reason), "{name}: {error}");
    }

    let canonical = checkpoint::encode(&complete_state(), &complete_policy()).unwrap();
    for (name, suffix) in [
        ("unknown field", vec![0x80, 0x06, 0x01]),
        ("duplicate known field", vec![0x08, 0x01]),
    ] {
        let mut bytes = canonical.clone();
        bytes.extend_from_slice(&suffix);
        let error = LegacyControlCheckpoint::decode(&bytes).err().unwrap();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{name}: {error}"
        );
        assert!(error.message().contains("noncanonical"), "{name}: {error}");
    }
}

fn append_varint(mut value: usize, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

#[test]
fn preflight_refuses_excessive_empty_entries_before_owned_state_validation() {
    let mut state = vec![
        0x08, 0x01, // format 1
        0x18, 0x01, // revision 1
        0x20, 0x01, // next_token 1
        0x28, 0x01, // next_action 1
        0x32, 0x00, // required empty topology
    ];
    for _ in 0..262_145 {
        state.extend_from_slice(&[0x42, 0x00]); // empty node entry
    }
    let mut bytes = vec![0x08, 0x01, 0x12];
    append_varint(state.len(), &mut bytes);
    bytes.extend_from_slice(&state);
    bytes.extend_from_slice(&[0x1a, 0x00]); // required policy

    let error = LegacyControlCheckpoint::decode(&bytes).err().unwrap();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(error.message().contains("262144 wire fields"), "{error}");
}

#[test]
fn preflight_refuses_truncated_lengths_varints_and_groups() {
    for (name, bytes, reason) in [
        (
            "truncated body",
            vec![0x12, 0x02, 0x08],
            "truncated protobuf field",
        ),
        (
            "truncated length",
            vec![0x12, 0x80],
            "malformed protobuf varint",
        ),
        (
            "unsupported group",
            vec![0x0b, 0x0c],
            "groups are unsupported",
        ),
    ] {
        let error = LegacyControlCheckpoint::decode(&bytes).err().unwrap();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{name}: {error}"
        );
        assert!(error.message().contains(reason), "{name}: {error}");
    }
}

#[test]
fn decode_refuses_input_beyond_the_checkpoint_byte_budget() {
    let bytes = vec![0; 16 * 1024 * 1024 + 1];
    let error = LegacyControlCheckpoint::decode(&bytes).err().unwrap();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(error.message().contains("16MiB"), "{error}");
}

#[test]
fn export_is_a_frozen_snapshot_and_refuses_a_latched_store() {
    let root = std::env::temp_dir().join(format!(
        "control-checkpoint-export-{}-{}",
        std::process::id(),
        now_ms()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("state.json");
    let plane = DurableControlPlane::open(&path, ControlPolicy::default()).unwrap();
    let before = plane.checkpoint_for_import().unwrap();
    assert_eq!(
        LegacyControlCheckpoint::decode(&before)
            .unwrap()
            .state()
            .revision,
        1
    );
    assert_eq!(
        plane
            .register(
                RegisterNodeRequest {
                    collection: String::new(),
                    node_id: "node-a".into(),
                    addr: "http://node-a".into(),
                    capacity: Some(NodeCapacity {
                        disk_bytes: 1000,
                        failure_domain: "a".into(),
                        ..Default::default()
                    }),
                    lease_ms: 10_000,
                },
                100
            )
            .unwrap()
            .control_revision,
        2
    );
    assert_eq!(
        LegacyControlCheckpoint::decode(&before)
            .unwrap()
            .state()
            .revision,
        1
    );
    assert_eq!(
        LegacyControlCheckpoint::decode(&plane.checkpoint_for_import().unwrap())
            .unwrap()
            .state()
            .revision,
        2
    );

    *plane.write_fault.lock().unwrap() = Some(StateWriteFault::AfterRename);
    assert_eq!(
        plane
            .register(
                RegisterNodeRequest {
                    collection: String::new(),
                    node_id: "node-b".into(),
                    addr: "http://node-b".into(),
                    capacity: Some(NodeCapacity {
                        disk_bytes: 1000,
                        failure_domain: "b".into(),
                        ..Default::default()
                    }),
                    lease_ms: 10_000,
                },
                100
            )
            .unwrap_err()
            .code(),
        tonic::Code::Internal
    );
    let error = plane.checkpoint_for_import().unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("persistence outcome is uncertain"),
        "{error}"
    );
    drop(plane);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn legacy_json_defaults_round_trip_and_unknown_nested_fields_refuse_without_rewrite() {
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
    let decoded = LegacyControlCheckpoint::decode(
        &checkpoint::encode(&old, &ControlPolicy::default()).unwrap(),
    )
    .unwrap();
    let restored = checkpoint::restore(decoded.state()).unwrap();
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
        "control-checkpoint-json-{}-{}",
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
fn legacy_json_duplicate_maps_and_sets_are_refused_instead_of_normalized() {
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
