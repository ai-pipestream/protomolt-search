use super::*;

#[test]
fn policy_replacement_is_resource_scoped_and_replay_requires_current_admin() {
    let dir = Directory::new("policy");
    let (store, authority) = create(&dir, limits());
    let revoke_alice = replace_grants(
        &authority,
        "books",
        b"revoke",
        1,
        1,
        vec![grant("bob", "books", AccessAction::Admin)],
    );
    let suppressed = store.execute("alice", &revoke_alice).unwrap_err();
    assert_eq!(suppressed.code(), Code::PermissionDenied);
    assert_eq!(
        store.execute("alice", &revoke_alice).unwrap_err().code(),
        Code::PermissionDenied
    );
    let visible = store.policy("bob", "workspace-a", "books").unwrap();
    assert_eq!(visible.revision, 2);
    assert_eq!(visible.resources.len(), 1);
    assert!(visible
        .grants
        .iter()
        .all(|grant| grant.collection == "books"));
    assert_eq!(
        store
            .policy("alice", "workspace-a", "books")
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );

    let restore = replace_grants(
        &authority,
        "books",
        b"restore",
        2,
        2,
        vec![
            grant("alice", "books", AccessAction::Admin),
            grant("bob", "books", AccessAction::Admin),
        ],
    );
    assert_eq!(store.execute("bob", &restore).unwrap().code, 0);
    let replay = store.execute("alice", &revoke_alice).unwrap();
    assert_eq!(
        (replay.code, replay.control_revision, replay.policy_revision),
        (0, 2, 2)
    );

    a_cross_collection_grant_is_a_recorded_rejection(&store, &authority);
}

fn a_cross_collection_grant_is_a_recorded_rejection(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
) {
    let cross = replace_grants(
        authority,
        "books",
        b"cross",
        3,
        3,
        vec![grant("bob", "music", AccessAction::Admin)],
    );
    let cross_rejection = store.execute("alice", &cross).unwrap();
    assert_eq!(cross_rejection.code, Code::PermissionDenied as u32);
    assert_eq!(store.execute("alice", &cross).unwrap(), cross_rejection);
    let corrected = replace_grants(
        authority,
        "books",
        b"cross",
        3,
        3,
        vec![
            grant("alice", "books", AccessAction::Admin),
            grant("bob", "books", AccessAction::Admin),
        ],
    );
    assert_eq!(
        store.execute("alice", &corrected).unwrap_err().code(),
        Code::FailedPrecondition
    );
    let corrected = replace_grants(
        authority,
        "books",
        b"cross-corrected",
        3,
        3,
        vec![
            grant("alice", "books", AccessAction::Admin),
            grant("bob", "books", AccessAction::Admin),
        ],
    );
    assert_eq!(store.execute("alice", &corrected).unwrap().code, 0);
    let music = store.policy("bob", "workspace-a", "music").unwrap();
    assert!(music
        .grants
        .iter()
        .any(|grant| grant.principal == "bob" && grant.collection == "music"));
}

#[test]
fn non_admin_and_cross_workspace_attempts_are_denied_without_reserving_command_ids() {
    let dir = Directory::new("authorization");
    let authority = identity(7);
    let mut initial_policy = policy(1);
    initial_policy.grants.extend([
        grant("reader", "books", AccessAction::Search),
        grant("ingester", "books", AccessAction::Ingest),
    ]);
    let store =
        SourceAuthorityStore::create(&dir.store(), &authority, &initial_policy, &limits()).unwrap();
    let owner = key(b"restricted-owner");

    non_admins_are_denied_every_operation(&store, &authority, &owner);

    let mut other_workspace = owner.clone();
    other_workspace.workspace = "workspace-b".into();
    let wrong_workspace = prepare(
        &authority,
        other_workspace,
        b"wrong-workspace",
        b"wrong-workspace-flow",
        (1, 1, 0),
        target("server", 171, SourceResidency::Server),
    );
    assert_eq!(
        store.execute("alice", &wrong_workspace).unwrap_err().code(),
        Code::PermissionDenied
    );

    let admit_reader = replace_grants(
        &authority,
        "books",
        b"admit-reader",
        1,
        1,
        vec![
            grant("alice", "books", AccessAction::Admin),
            grant("reader", "books", AccessAction::Admin),
        ],
    );
    assert_eq!(store.execute("alice", &admit_reader).unwrap().code, 0);
    let admitted = prepare(
        &authority,
        owner,
        b"denied-command",
        b"denied-workflow",
        (2, 2, 0),
        target("server", 161, SourceResidency::Server),
    );
    assert_eq!(store.execute("reader", &admitted).unwrap().code, 0);
}

fn non_admins_are_denied_every_operation(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    owner: &LogicalSourceOwner,
) {
    for principal in ["reader", "ingester"] {
        let command = prepare(
            authority,
            owner.clone(),
            b"denied-command",
            b"denied-workflow",
            (1, 1, 0),
            target("server", 161, SourceResidency::Server),
        );
        assert_eq!(
            store.execute(principal, &command).unwrap_err().code(),
            Code::PermissionDenied
        );
        assert_eq!(
            store.owner(principal, owner).unwrap_err().code(),
            Code::PermissionDenied
        );
        assert_eq!(
            store
                .decision(principal, owner, b"denied-command")
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            store
                .policy(principal, "workspace-a", "books")
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
}

#[test]
fn policy_revision_exhaustion_is_a_durable_unchanged_state_decision() {
    let dir = Directory::new("policy-revision-exhaustion");
    let authority = identity(7);
    let initial_policy = policy(u64::MAX);
    let store =
        SourceAuthorityStore::create(&dir.store(), &authority, &initial_policy, &limits()).unwrap();
    let command = replace_grants(
        &authority,
        "books",
        b"exhaust-policy-revision",
        1,
        u64::MAX,
        vec![grant("alice", "books", AccessAction::Admin)],
    );
    let rejected = store.execute("alice", &command).unwrap();
    assert_eq!(rejected.code, Code::ResourceExhausted as u32);
    assert!(rejected.message.contains("policy revision exhausted"));
    assert_eq!(rejected.control_revision, 1);
    assert_eq!(rejected.policy_revision, u64::MAX);
    assert_eq!(store.execute("alice", &command).unwrap(), rejected);
    assert_eq!(
        store
            .policy("alice", "workspace-a", "books")
            .unwrap()
            .revision,
        u64::MAX
    );

    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.store(), &authority).unwrap();
    assert_eq!(
        reopened
            .decision("alice", command.key.as_ref().unwrap(), &command.command_id)
            .unwrap(),
        rejected
    );
    assert_eq!(
        reopened
            .policy("alice", "workspace-a", "books")
            .unwrap()
            .revision,
        u64::MAX
    );
}
