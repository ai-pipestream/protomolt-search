use super::*;

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

type Mutation = Box<dyn Fn(&mut ControlInstallState)>;

/// States whose topology, nodes, replicas or actions are invalid, each
/// with the text its refusal names.
fn invalid_facts() -> Vec<(&'static str, Mutation)> {
    vec![
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
    ]
}

/// States whose configuration is invalid, each with the text its refusal
/// names.
fn invalid_configurations() -> Vec<(&'static str, Mutation)> {
    vec![
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
    ]
}

#[test]
fn invalid_states_refuse_directly_and_record_nothing() {
    let dir = Directory::new("invalid");
    let authority = identity(7);
    let store = store(&dir, 7, &limits(64 << 10, 64));
    let mut cases = invalid_facts();
    cases.extend(invalid_configurations());
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
