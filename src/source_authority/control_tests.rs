use super::control::*;
use super::*;
use crate::pb::storage::{
    control_configuration::{Derived, Placement, Provider},
    source_authority_command::Action,
};
use crate::pb::{
    AccessAction, AccessPolicy, ClusterNodeState, CollectionGrant, CollectionResource,
    NodeCapacity, NodeResidency, PlacementActionKind, PlacementNode, PlacementTree,
    ShardReplicaRole,
};
use crate::test_support::ForkGuarded;
use redb::{ReadableTable, TableHandle};
use std::path::PathBuf;
use tonic::Code;

pub(crate) struct Directory(pub(crate) PathBuf);

impl Directory {
    pub(crate) fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "control-install-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    pub(crate) fn authority(&self) -> PathBuf {
        self.0.join("authority.redb")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

pub(crate) fn grant(principal: &str, collection: &str) -> CollectionGrant {
    CollectionGrant {
        principal: principal.into(),
        workspace: "workspace-a".into(),
        collection: collection.into(),
        actions: vec![AccessAction::Admin as i32],
        ..Default::default()
    }
}

pub(crate) fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 1,
        revision: 1,
        resources: ["books", "music"]
            .map(|collection| CollectionResource {
                workspace: "workspace-a".into(),
                collection: collection.into(),
            })
            .to_vec(),
        grants: vec![
            grant("alice", "books"),
            grant("bob", "books"),
            grant("alice", "music"),
        ],
    }
}

pub(crate) fn limits(max_command_bytes: u32, max_decisions: u64) -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions,
        max_payload_bytes: 64 << 20,
        max_command_bytes,
    }
}

pub(crate) fn key(collection: &str) -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: collection.into(),
        owner_id: Vec::new(),
    }
}

pub(crate) fn store(
    dir: &Directory,
    seed: u8,
    limits: &SourceAuthorityLimits,
) -> SourceAuthorityStore {
    SourceAuthorityStore::create(&dir.authority(), &identity(seed), &policy(), limits).unwrap()
}

pub(crate) fn geometry() -> ControlProviderGeometry {
    ControlProviderGeometry {
        format_version: 1,
        backend_kind: "embedded-turbovec".into(),
        backend_version: "s21".into(),
        config_format: "turbovec-config-v1".into(),
        config_payload: vec![1, 2, 3],
        dimension: 384,
        bits_per_component: 4,
        row_bytes_formula_version: 1,
        scoring_fingerprint: "fp".into(),
    }
}

/// No placement, no derived columns, explicit provider geometry.
pub(crate) fn configuration() -> ControlConfiguration {
    ControlConfiguration {
        format_version: 1,
        placement: Some(Placement::NoPlacement(true)),
        route_codes: Vec::new(),
        derived: Some(Derived::NoDerived(true)),
        derived_fingerprint: String::new(),
        provider: Some(Provider::Geometry(geometry())),
        policy: Some(ControlPlannerPolicy {
            format_version: 1,
            planner_version: 1,
            reconcile: Some(ControlReconcilePolicy {
                lease_ms: 31_337,
                replication_factor: 3,
                split_rows: 123_456,
                merge_rows: 7_654,
                compact_segments: 11,
                compact_tombstone_ppm: 222_333,
                history_limit: 17,
            }),
        }),
    }
}

fn route(addr: &str, replica: Option<&str>, hash_lo: u64, hash_hi: u64) -> ControlRoute {
    ControlRoute {
        addr: addr.into(),
        replica: replica.map(str::to_owned),
        hash_lo,
        hash_hi,
    }
}

/// Two nodes (node/α server, node z device), shard-a on both (routes
/// 0..=99, primary node/α at generation 7), one pending copy, completed ids
/// 3 and 9, and generation 3 with `routes` current routes (at least 2)
/// after history generations 1 and 2.
pub(crate) fn state(routes: usize) -> ControlInstallState {
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

pub(crate) fn command(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control: u64,
    state: ControlInstallState,
) -> ControlInstallCommand {
    ControlInstallCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control,
        expected_policy_revision: 1,
        state: Some(state),
    }
}

/// Install `state` for books at `revision`; returns the receipt.
pub(crate) fn install(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    state: ControlInstallState,
    revision: u64,
) -> ControlInstallReceipt {
    let decision = store
        .install_control("alice", &command(authority, "install", revision, state))
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    decision.receipt.unwrap()
}

pub(crate) type TableRows = Vec<(Vec<u8>, Vec<u8>)>;

pub(crate) fn table_bytes(store: &SourceAuthorityStore) -> Vec<(String, TableRows)> {
    let tx = store.inner.database().begin_read().unwrap();
    let mut names: Vec<String> = tx
        .list_tables()
        .unwrap()
        .map(|t| t.name().to_string())
        .collect();
    names.sort();
    names
        .into_iter()
        .map(|name: String| {
            let rows = if name == META.name() {
                let table = tx.open_table(META).unwrap();
                table
                    .iter()
                    .unwrap()
                    .map(|e| {
                        let (k, v) = e.unwrap();
                        (k.value().as_bytes().to_vec(), v.value().to_vec())
                    })
                    .collect()
            } else {
                let definition = redb::TableDefinition::<&[u8], &[u8]>::new(name.as_str());
                let table = tx.open_table(definition).unwrap();
                table
                    .iter()
                    .unwrap()
                    .map(|e| {
                        let (k, v) = e.unwrap();
                        (k.value().to_vec(), v.value().to_vec())
                    })
                    .collect()
            };
            (name, rows)
        })
        .collect()
}

#[test]
fn an_install_writes_every_row_and_the_snapshot_survives_reopen() {
    let dir = Directory::new("round-trip");
    let authority = identity(7);
    let store = store(&dir, 7, &limits(64 << 10, 64));
    let state = state(40);
    let receipt = install(&store, &authority, state.clone(), 1);
    assert_eq!(
        receipt,
        ControlInstallReceipt {
            format_version: 1,
            control_revision: 2,
            generation: 3,
            routes: 40,
            history: 2,
            nodes: 2,
            replicas: 2,
            actions: 1,
            completed_actions: 2,
        }
    );

    let snapshot = store.control_snapshot("alice", &key("books")).unwrap();
    let applied = snapshot.state.as_ref().unwrap();
    assert_eq!(applied.next_token, u64::MAX);
    assert_eq!(applied.next_action, u64::MAX);
    assert_eq!(applied.topology_generation, 3);
    assert_eq!(applied.history_generations, [1, 2]);
    assert_eq!(applied.configuration, Some(configuration()));
    let topology = snapshot.topology.as_ref().unwrap();
    assert_eq!(topology.routes, state.topology.as_ref().unwrap().routes);
    assert!(topology.current && !topology.codes_available);
    // Residency is the node's own declaration, copied exactly.
    let residency: Vec<(String, i32)> = snapshot
        .nodes
        .iter()
        .map(|n| (n.node_id.clone(), n.residency))
        .collect();
    assert_eq!(
        residency,
        [
            ("node z".to_string(), SourceResidency::DeviceLocal as i32),
            ("node/α".to_string(), SourceResidency::Server as i32),
        ]
    );
    assert_eq!(snapshot.replicas.len(), 2);
    assert_eq!(snapshot.actions.len(), 1);
    assert_eq!(
        snapshot.actions[0].state,
        ControlActionState::Unreconciled as i32
    );
    assert_eq!(snapshot.completed_actions, [3, 9]);
    assert_eq!(snapshot.control_revision, 2);
    assert_eq!(snapshot.digest.len(), 32);

    // An exact retry answers with the stored decision and changes nothing.
    let retry = store
        .install_control("alice", &command(&authority, "install", 1, state.clone()))
        .unwrap();
    assert_eq!(retry.receipt, Some(receipt));
    assert_eq!(
        store.control_snapshot("alice", &key("books")).unwrap(),
        snapshot
    );

    // Reopen recomputes every count and byte; the committed view is identical.
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(
        reopened.control_snapshot("alice", &key("books")).unwrap(),
        snapshot
    );
    // A resource is installed once; the refusal is recorded.
    let again = reopened
        .install_control("alice", &command(&authority, "install-2", 2, state))
        .unwrap();
    assert_eq!(again.code, Code::AlreadyExists as u32, "{}", again.message);
    assert_eq!(again.control_revision, 2);
    drop(reopened);
    SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
}

#[test]
fn retries_revisions_actors_and_the_command_namespace_are_pinned() {
    let dir = Directory::new("retries");
    let authority = identity(7);
    let store = store(&dir, 7, &limits(64 << 10, 64));

    // A stale control revision is a recorded rejection with an exact retry.
    let stale = command(&authority, "stale", 9, state(2));
    let first = store.install_control("alice", &stale).unwrap();
    assert_eq!(first.code, Code::FailedPrecondition as u32);
    assert!(
        first.message.contains("control revision"),
        "{}",
        first.message
    );
    assert_eq!(first.control_revision, 1);
    assert!(first.receipt.is_none());
    assert_eq!(store.install_control("alice", &stale).unwrap(), first);
    assert_eq!(
        store
            .control_install_decision("alice", &key("books"), b"stale")
            .unwrap(),
        first
    );
    // The same command_id with other content refuses directly.
    let changed = command(&authority, "stale", 1, state(2));
    assert_eq!(
        store.install_control("alice", &changed).unwrap_err().code(),
        Code::FailedPrecondition
    );
    // A stale policy revision is recorded the same way.
    let mut policy_stale = command(&authority, "policy", 1, state(2));
    policy_stale.expected_policy_revision = 2;
    let decision = store.install_control("alice", &policy_stale).unwrap();
    assert_eq!(decision.code, Code::FailedPrecondition as u32);
    assert!(decision.message.contains("policy revision"));

    // An actor without Admin, or another authority, refuses and records nothing.
    let foreign = command(&authority, "carol", 1, state(2));
    assert_eq!(
        store.install_control("carol", &foreign).unwrap_err().code(),
        Code::PermissionDenied
    );
    let other = command(&identity(9), "other", 1, state(2));
    assert_eq!(
        store.install_control("alice", &other).unwrap_err().code(),
        Code::FailedPrecondition
    );

    // A source command cannot reuse a command_id an install used.
    let installed = store
        .install_control("alice", &command(&authority, "shared", 1, state(2)))
        .unwrap();
    assert_eq!(installed.code, 0, "{}", installed.message);
    // The namespace is (actor, key, command_id): the same collection key.
    let regrant = SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        command_id: b"shared".to_vec(),
        expected_control_revision: 2,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: vec![grant("alice", "books")],
        })),
    };
    let error = store.execute("alice", &regrant).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("already used"), "{error}");
    drop(store);
    SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
}

#[test]
fn invalid_states_refuse_directly_and_record_nothing() {
    let dir = Directory::new("invalid");
    let authority = identity(7);
    let store = store(&dir, 7, &limits(64 << 10, 64));
    type Mutation = Box<dyn Fn(&mut ControlInstallState)>;
    let cases: Vec<(&str, Mutation)> = vec![
        ("allocators", Box::new(|s| s.next_token = 0)),
        ("topology is required", Box::new(|s| s.topology = None)),
        (
            "generation repeats",
            Box::new(|s| s.history[0].generation = 3),
        ),
        (
            "reversed hash range",
            Box::new(|s| {
                let r = &mut s.topology.as_mut().unwrap().routes[0];
                std::mem::swap(&mut r.hash_lo, &mut r.hash_hi);
            }),
        ),
        ("node_id order", Box::new(|s| s.nodes.reverse())),
        (
            "node state",
            Box::new(|s| s.nodes[0].state = ClusterNodeState::Unspecified as i32),
        ),
        (
            "lease token",
            Box::new(|s| s.nodes[1].lease_token = s.nodes[0].lease_token),
        ),
        ("node capacity", Box::new(|s| s.nodes[0].capacity = None)),
        (
            "residency",
            Box::new(|s| {
                s.nodes[0].capacity.as_mut().unwrap().residency = 99;
            }),
        ),
        (
            "(shard_id, node_id) order",
            Box::new(|s| s.replicas.reverse()),
        ),
        (
            "replica role",
            Box::new(|s| s.replicas[0].role = ShardReplicaRole::Unspecified as i32),
        ),
        (
            "tombstone",
            Box::new(|s| s.replicas[0].tombstones = u64::MAX),
        ),
        (
            "completed actions",
            Box::new(|s| s.completed_actions = vec![9, 3]),
        ),
        (
            "pending action id",
            Box::new(|s| s.actions[0].action_id = 9),
        ),
        (
            "action kind",
            Box::new(|s| s.actions[0].kind = PlacementActionKind::Unspecified as i32),
        ),
        (
            "configuration is required",
            Box::new(|s| s.configuration = None),
        ),
        (
            "placement must be an explicit tree",
            Box::new(|s| s.configuration.as_mut().unwrap().placement = None),
        ),
        (
            "route codes are present",
            Box::new(|s| {
                s.configuration
                    .as_mut()
                    .unwrap()
                    .route_codes
                    .push(PlacementRouteCode::default());
            }),
        ),
        (
            "derived fingerprint present",
            Box::new(|s| s.configuration.as_mut().unwrap().derived_fingerprint = "x".into()),
        ),
        (
            "provider geometry",
            Box::new(|s| {
                if let Some(Provider::Geometry(g)) =
                    s.configuration.as_mut().unwrap().provider.as_mut()
                {
                    g.dimension = 7;
                }
            }),
        ),
        (
            "planner policy",
            Box::new(|s| s.configuration.as_mut().unwrap().policy = None),
        ),
    ];
    for (index, (expected, mutate)) in cases.iter().enumerate() {
        let mut state = state(2);
        mutate(&mut state);
        let id = format!("bad-{index}");
        let error = store
            .install_control("alice", &command(&authority, &id, 1, state))
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument, "{expected}: {error}");
        assert!(error.message().contains(expected), "{expected}: {error}");
        assert_eq!(
            store
                .control_install_decision("alice", &key("books"), id.as_bytes())
                .unwrap_err()
                .code(),
            Code::NotFound
        );
    }
    // A command above max_command_bytes refuses before anything else.
    let small = Directory::new("invalid-small");
    let small_store = SourceAuthorityStore::create(
        &small.authority(),
        &authority,
        &policy(),
        &limits(1 << 10, 64),
    )
    .unwrap();
    assert_eq!(
        small_store
            .install_control("alice", &command(&authority, "big", 1, state(40)))
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    drop(store);
    SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
}

#[test]
fn replaying_the_retained_commands_reproduces_the_identical_store() {
    let dir = Directory::new("replay");
    let authority = identity(7);
    let limits = limits(64 << 10, 128);
    let store = store(&dir, 7, &limits);
    store
        .install_control("alice", &command(&authority, "stale", 5, state(2)))
        .unwrap();
    install(&store, &authority, state(30), 1);
    let recorded: Vec<ControlInstallCommand> = {
        let tx = store.inner.database().begin_read().unwrap();
        let ops = tx.open_table(INSTALL_OPERATIONS).unwrap();
        let mut operations: Vec<ControlInstallOperation> = ops
            .iter()
            .unwrap()
            .map(|e| contract::decode(e.unwrap().1.value()).unwrap())
            .collect();
        operations.sort_by_key(|op| op.decision.as_ref().unwrap().code == 0);
        operations
            .into_iter()
            .map(|op| op.command.unwrap())
            .collect()
    };
    let replica_dir = Directory::new("replay-replica");
    let replica =
        SourceAuthorityStore::create(&replica_dir.authority(), &authority, &policy(), &limits)
            .unwrap();
    for command in &recorded {
        replica.replay_control_install("alice", command).unwrap();
    }
    assert_eq!(table_bytes(&replica), table_bytes(&store));
}

#[test]
fn install_exit_worker() {
    let Some(fault) = std::env::var_os("PSEARCH_CONTROL_INSTALL_EXIT_FAULT") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("PSEARCH_CONTROL_INSTALL_DIR").unwrap());
    let authority = identity(7);
    let store = SourceAuthorityStore::create(
        &root.join("authority.redb"),
        &authority,
        &policy(),
        &limits(64 << 10, 64),
    )
    .unwrap();
    *store.inner.fault.lock().unwrap() = Some(match fault.to_str().unwrap() {
        "before" => Fault::ExitBeforeCommit,
        "after" => Fault::ExitAfterCommit,
        other => panic!("unknown exit fault {other}"),
    });
    let _ = store.install_control("alice", &command(&authority, "install", 1, state(2)));
    panic!("exit fault did not terminate the worker");
}

#[test]
fn an_abrupt_exit_around_the_install_commit_recovers_and_converges() {
    for fault in ["before", "after"] {
        let dir = Directory::new(&format!("exit-{fault}"));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("source_authority::control_tests::install_exit_worker")
            .arg("--nocapture")
            .env("PSEARCH_CONTROL_INSTALL_EXIT_FAULT", fault)
            .env("PSEARCH_CONTROL_INSTALL_DIR", &dir.0)
            .status_guarded()
            .unwrap();
        assert_eq!(status.code(), Some(87), "{fault}");
        let authority = identity(7);
        let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
        let stored = reopened.control_install_decision("alice", &key("books"), b"install");
        if fault == "after" {
            assert_eq!(stored.unwrap().code, 0);
        } else {
            assert_eq!(stored.unwrap_err().code(), Code::NotFound);
        }
        // The retry converges either way: applied once, at revision 2.
        let decision = reopened
            .install_control("alice", &command(&authority, "install", 1, state(2)))
            .unwrap();
        assert_eq!(decision.code, 0, "{fault}: {}", decision.message);
        assert_eq!(decision.control_revision, 2);
        let snapshot = reopened.control_snapshot("alice", &key("books")).unwrap();
        drop(reopened);
        let again = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
        assert_eq!(
            again.control_snapshot("alice", &key("books")).unwrap(),
            snapshot
        );
    }
}

#[test]
fn a_store_whose_control_rows_disagree_with_the_headers_refuses_to_open() {
    let authority = identity(7);
    type Tamper = fn(&redb::WriteTransaction);
    let tamper: [(&str, Tamper); 4] = [
        ("format", |tx| {
            let mut meta = tx.open_table(META).unwrap();
            let mut header: ControlStoreHeader =
                contract::decode(meta.get(CONTROL_HEADER).unwrap().unwrap().value()).unwrap();
            header.format_version = 2;
            meta.insert(CONTROL_HEADER, header.encode_to_vec().as_slice())
                .unwrap();
        }),
        ("count", |tx| {
            let mut meta = tx.open_table(META).unwrap();
            let mut header: ControlStoreHeader =
                contract::decode(meta.get(CONTROL_HEADER).unwrap().unwrap().value()).unwrap();
            header.node_count += 1;
            meta.insert(CONTROL_HEADER, header.encode_to_vec().as_slice())
                .unwrap();
        }),
        ("orphan fact", |tx| {
            let mut nodes = tx.open_table(NODES).unwrap();
            let k = ControlNodeKey {
                key: Some(key("music")),
                node_id: "ghost".into(),
            };
            nodes
                .insert(k.encode_to_vec().as_slice(), [].as_slice())
                .unwrap();
        }),
        ("extra table", |tx| {
            tx.open_table(redb::TableDefinition::<&[u8], &[u8]>::new("stray"))
                .unwrap();
        }),
    ];
    for (name, apply) in tamper {
        let dir = Directory::new(&format!("tamper-{name}"));
        let store = store(&dir, 7, &limits(64 << 10, 64));
        install(&store, &authority, state(2), 1);
        {
            let tx = store.inner.database().begin_write().unwrap();
            apply(&tx);
            tx.commit().unwrap();
        }
        drop(store);
        let error = SourceAuthorityStore::open(&dir.authority(), &authority)
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::DataLoss, "{name}: {error}");
    }
}

fn recovery_exact_value<T: Message + Default>(
    tx: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    mutate: impl FnOnce(&mut T),
) {
    let mut table = tx.open_table(definition).unwrap();
    let (key, bytes) = {
        let entry = table.iter().unwrap().next().unwrap().unwrap();
        (entry.0.value().to_vec(), entry.1.value().to_vec())
    };
    let mut value: T = contract::decode(&bytes).unwrap();
    mutate(&mut value);
    let changed = value.encode_to_vec();
    assert_eq!(
        changed.len(),
        bytes.len(),
        "substitution must preserve payload accounting"
    );
    table.insert(key.as_slice(), changed.as_slice()).unwrap();
}
fn recovery_exact_key<T: Message + Default>(
    tx: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    mutate: impl FnOnce(&mut T),
) {
    let mut table = tx.open_table(definition).unwrap();
    let (key, value) = {
        let entry = table.iter().unwrap().next().unwrap().unwrap();
        (entry.0.value().to_vec(), entry.1.value().to_vec())
    };
    let mut decoded: T = contract::decode(&key).unwrap();
    mutate(&mut decoded);
    let changed = decoded.encode_to_vec();
    assert_eq!(changed.len(), key.len());
    table.remove(key.as_slice()).unwrap();
    assert!(table
        .insert(changed.as_slice(), value.as_slice())
        .unwrap()
        .is_none());
}
fn recovery_exact_tamper(name: &str, mutate: impl FnOnce(&redb::WriteTransaction)) {
    let dir = Directory::new(name);
    let authority = identity(7);
    let store = store(&dir, 7, &limits(64 << 10, 64));
    install(&store, &authority, state(2), 1);
    let before = table_bytes(&store);
    {
        let tx = store.inner.database().begin_write().unwrap();
        mutate(&tx);
        tx.commit().unwrap();
    }
    // The original accepted commands, receipts, headers and declared accounting
    // are untouched. Equal-size tests also keep every table's byte total.
    let after = table_bytes(&store);
    for name in [META.name(), INSTALL_OPERATIONS.name()] {
        assert_eq!(
            before.iter().find(|(n, _)| n == name),
            after.iter().find(|(n, _)| n == name)
        );
    }
    drop(store);
    let error = SourceAuthorityStore::open(&dir.authority(), &authority)
        .err()
        .unwrap_or_else(|| panic!("recovery accepted substituted {name}"));
    assert_eq!(error.code(), Code::DataLoss, "{name}: {error}");
}

#[test]
fn recovery_exact_topology_current_address_substitution() {
    recovery_exact_tamper("current-topology", |tx| {
        let mut table = tx.open_table(TOPOLOGIES).unwrap();
        let key = ControlTopologyKey {
            key: Some(key("books")),
            generation: 3,
        }
        .encode_to_vec();
        let old = table.get(key.as_slice()).unwrap().unwrap().value().to_vec();
        let mut row: ControlTopology = contract::decode(&old).unwrap();
        row.routes[0].addr.replace_range(0..1, "x");
        let changed = row.encode_to_vec();
        assert_eq!(changed.len(), old.len());
        table.insert(key.as_slice(), changed.as_slice()).unwrap();
    });
}
#[test]
fn recovery_exact_topology_history_address_substitution() {
    recovery_exact_tamper("history-topology", |tx| {
        recovery_exact_value::<ControlTopology>(tx, TOPOLOGIES, |row| {
            row.routes[0].addr.replace_range(0..1, "x")
        })
    });
}
#[test]
fn recovery_exact_node_value_substitution() {
    recovery_exact_tamper("node-value", |tx| {
        recovery_exact_value::<ControlNode>(tx, NODES, |row| {
            row.node.as_mut().unwrap().addr.replace_range(0..1, "x")
        })
    });
}
#[test]
fn recovery_exact_node_key_substitution() {
    recovery_exact_tamper("node-key", |tx| {
        recovery_exact_key::<ControlNodeKey>(tx, NODES, |row| row.node_id.replace_range(0..1, "x"))
    });
}
#[test]
fn recovery_exact_replica_value_substitution() {
    recovery_exact_tamper("replica-value", |tx| {
        recovery_exact_value::<ControlReplica>(tx, REPLICAS, |row| {
            row.replica
                .as_mut()
                .unwrap()
                .analysis_fingerprint
                .replace_range(0..1, "x")
        })
    });
}
#[test]
fn recovery_exact_replica_key_substitution() {
    recovery_exact_tamper("replica-key", |tx| {
        recovery_exact_key::<ControlReplicaKey>(tx, REPLICAS, |row| {
            row.replica_key.replace_range(0..1, "x")
        })
    });
}
#[test]
fn recovery_exact_action_value_substitution() {
    recovery_exact_tamper("action-value", |tx| {
        recovery_exact_value::<ControlAction>(tx, ACTIONS, |row| {
            row.action.as_mut().unwrap().reason.replace_range(0..1, "x")
        })
    });
}
#[test]
fn recovery_exact_action_key_substitution() {
    recovery_exact_tamper("action-key", |tx| {
        recovery_exact_key::<ControlActionKey>(tx, ACTIONS, |row| row.action_id -= 1)
    });
}
#[test]
fn recovery_exact_completed_action_key_substitution() {
    recovery_exact_tamper("completed-key", |tx| {
        recovery_exact_key::<ControlActionKey>(tx, COMPLETED, |row| row.action_id += 1)
    });
}
#[test]
fn recovery_exact_configuration_provider_substitution() {
    recovery_exact_tamper("provider-config", |tx| {
        recovery_exact_value::<ControlCollectionState>(tx, CONTROL, |row| {
            if let Some(Provider::Geometry(geometry)) =
                row.configuration.as_mut().unwrap().provider.as_mut()
            {
                geometry.config_payload[0] ^= 1;
            } else {
                panic!("expected provider geometry");
            }
        })
    });
}
#[test]
fn recovery_exact_configuration_policy_substitution() {
    recovery_exact_tamper("planner-config", |tx| {
        recovery_exact_value::<ControlCollectionState>(tx, CONTROL, |row| {
            row.configuration
                .as_mut()
                .unwrap()
                .policy
                .as_mut()
                .unwrap()
                .reconcile
                .as_mut()
                .unwrap()
                .lease_ms -= 1
        })
    });
}
#[test]
fn recovery_exact_allocator_substitution() {
    recovery_exact_tamper("allocator", |tx| {
        recovery_exact_value::<ControlCollectionState>(tx, CONTROL, |row| row.next_token -= 1)
    });
}
#[test]
fn recovery_exact_missing_or_extra_fact_refuses() {
    for extra in [false, true] {
        recovery_exact_tamper(if extra { "extra" } else { "missing" }, |tx| {
            let mut table = tx.open_table(COMPLETED).unwrap();
            let key = ControlActionKey {
                key: Some(key("books")),
                action_id: if extra { 10 } else { 3 },
            }
            .encode_to_vec();
            if extra {
                table
                    .insert(
                        key.as_slice(),
                        ControlCompletedAction { format_version: 1 }
                            .encode_to_vec()
                            .as_slice(),
                    )
                    .unwrap();
            } else {
                table.remove(key.as_slice()).unwrap();
            }
        });
    }
}

#[test]
fn recovery_exact_valid_install_survives_later_superseding_capacity_and_policy_updates() {
    let dir = Directory::new("later-control-updates");
    let authority = identity(7);
    let store = store(&dir, 7, &limits(64 << 10, 64));
    install(&store, &authority, state(2), 1);
    let initial = store.control_snapshot("alice", &key("books")).unwrap();
    let mut configuration = CapacityConfiguration {
        format_version: 1,
        cohort_length_ms: 60_000,
        cohort_phase_unix_ms: 0,
        max_total_records: 64,
        max_total_bytes: 1 << 20,
        max_registered_nodes: 8,
        policy: Some(CapacityTierPolicy {
            format_version: 1,
            tiers: vec![CapacityTierSpec {
                name: "first".into(),
                residency: CapacityResidency::Server as i32,
                min_replicas: 1,
                scans_per_byte_nanos_lo: 0,
                scans_per_byte_nanos_hi: 1_000_000_000,
                max_seconds_since_scan: 3600,
            }],
        }),
    };
    for (revision, id) in [(2, "first-config"), (3, "latest-config")] {
        configuration.policy.as_mut().unwrap().tiers[0].name = id.into();
        let accepted = store
            .configure_capacity(
                "alice",
                &CapacityConfigureCommand {
                    format_version: 1,
                    authority: Some(authority.clone()),
                    key: Some(key("books")),
                    command_id: id.as_bytes().to_vec(),
                    expected_control_revision: revision,
                    expected_policy_revision: 1,
                    configuration: Some(configuration.clone()),
                },
            )
            .unwrap();
        assert_eq!(accepted.code, 0, "{}", accepted.message);
    }
    let policy = SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        command_id: b"later-grants".to_vec(),
        expected_control_revision: 4,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: vec![grant("alice", "books")],
        })),
    };
    assert_eq!(store.execute("alice", &policy).unwrap().code, 0);
    let bytes = table_bytes(&store);
    let expected = store.control_snapshot("alice", &key("books")).unwrap();
    assert_eq!(expected.state, initial.state);
    assert_eq!(expected.control_revision, 5);
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(
        reopened.control_snapshot("alice", &key("books")).unwrap(),
        expected
    );
    assert_eq!(
        reopened
            .capacity_state("alice", &key("books"))
            .unwrap()
            .configuration,
        Some(configuration)
    );
    assert_eq!(
        table_bytes(&reopened),
        bytes,
        "recovery must not repair or rewrite accepted state"
    );
}
