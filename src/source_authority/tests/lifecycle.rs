use super::*;

#[test]
fn preparation_cancel_reprepare_and_workflow_reservation_persist() {
    let dir = Directory::new("lifecycle");
    let (store, authority) = create(&dir, limits());
    let owner = key(b"phone-owner");
    let local = target("phone-17", 31, SourceResidency::DeviceLocal);
    let first_command = prepare(
        &authority,
        owner.clone(),
        b"prepare-1",
        b"workflow-1",
        (1, 1, 0),
        local.clone(),
    );
    let first = store.execute("alice", &first_command).unwrap();
    assert_eq!(first.code, 0);
    let prepared = first.owner.clone().unwrap();
    assert_eq!(prepared.key, Some(owner.clone()));
    assert_eq!(prepared.target, Some(local.clone()));
    assert_eq!(prepared.ownership_generation, 1);
    assert_eq!(prepared.phase, PreparedSourceOwnerPhase::Prepared as i32);
    let cancelled = store
        .execute(
            "alice",
            &cancel(
                &authority,
                owner.clone(),
                b"cancel-1",
                b"workflow-1",
                2,
                1,
                1,
            ),
        )
        .unwrap();
    assert_eq!(cancelled.code, 0);
    assert_eq!(
        cancelled.owner.as_ref().unwrap().phase,
        PreparedSourceOwnerPhase::Cancelled as i32
    );

    a_cancelled_owner_keeps_its_history_and_residency(&store, &authority, &owner, &local);
    let (cancelled_again, reused) =
        a_cancelled_workflow_stays_reserved_across_generations(&store, &authority, &owner, &local);

    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.store(), &authority).unwrap();
    assert_eq!(
        reopened.owner("alice", &owner).unwrap(),
        cancelled_again.owner.unwrap()
    );
    assert_eq!(
        reopened.decision("alice", &owner, b"reuse").unwrap(),
        reused
    );
}

fn a_cancelled_owner_keeps_its_history_and_residency(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    owner: &LogicalSourceOwner,
    local: &SourceStorageTarget,
) {
    let mut changed_residency = local.clone();
    changed_residency.residency = SourceResidency::Server as i32;
    changed_residency.resident_device_id.clear();
    let mut changed_device = local.clone();
    changed_device.node_id = "phone-18".into();
    changed_device.resident_device_id = "phone-18".into();
    let mut changed_history = local.clone();
    changed_history.history_id = vec![53; 16];
    for (command_id, invalid) in [
        (b"change-residency".as_slice(), changed_residency),
        (b"change-device".as_slice(), changed_device),
        (b"change-history".as_slice(), changed_history),
    ] {
        let rejected = store
            .execute(
                "alice",
                &prepare(
                    authority,
                    owner.clone(),
                    command_id,
                    b"invalid-flow",
                    (3, 1, 1),
                    invalid,
                ),
            )
            .unwrap();
        assert_ne!(rejected.code, 0);
        assert_eq!(rejected.control_revision, 3);
    }
}

fn a_cancelled_workflow_stays_reserved_across_generations(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    owner: &LogicalSourceOwner,
    local: &SourceStorageTarget,
) -> (SourceAuthorityDecision, SourceAuthorityDecision) {
    let mut replacement = local.clone();
    replacement.storage_incarnation = vec![51; 16];
    let second = store
        .execute(
            "alice",
            &prepare(
                authority,
                owner.clone(),
                b"prepare-2",
                b"workflow-2",
                (3, 1, 1),
                replacement.clone(),
            ),
        )
        .unwrap();
    assert_eq!(second.code, 0);
    assert_eq!(second.owner.as_ref().unwrap().ownership_generation, 2);
    assert_eq!(
        second.owner.as_ref().unwrap().target,
        Some(replacement.clone())
    );
    let cancelled_again = store
        .execute(
            "alice",
            &cancel(
                authority,
                owner.clone(),
                b"cancel-2",
                b"workflow-2",
                4,
                1,
                2,
            ),
        )
        .unwrap();
    assert_eq!(cancelled_again.code, 0);
    let mut another_incarnation = replacement;
    another_incarnation.storage_incarnation = vec![71; 16];
    let reused_command = prepare(
        authority,
        owner.clone(),
        b"reuse",
        b"workflow-1",
        (5, 1, 2),
        another_incarnation,
    );
    let reused = store.execute("alice", &reused_command).unwrap();
    assert_ne!(reused.code, 0);
    assert_eq!(store.execute("alice", &reused_command).unwrap(), reused);
    (cancelled_again, reused)
}

#[test]
fn complete_owner_actor_and_authority_scopes_do_not_alias() {
    let dir = Directory::new("scopes");
    let (store, authority) = create(&dir, limits());
    let phone = key(b"phone");
    let tablet = key(b"tablet");
    let command_id = b"same-command";
    let a = store
        .execute(
            "alice",
            &prepare(
                &authority,
                phone.clone(),
                command_id,
                b"phone-flow",
                (1, 1, 0),
                target("phone", 11, SourceResidency::DeviceLocal),
            ),
        )
        .unwrap();
    let b = store
        .execute(
            "alice",
            &prepare(
                &authority,
                tablet.clone(),
                command_id,
                b"tablet-flow",
                (2, 1, 0),
                target("tablet", 21, SourceResidency::DeviceLocal),
            ),
        )
        .unwrap();
    assert_eq!(a.code, 0);
    assert_eq!(b.code, 0);
    let actor_rejection = store
        .execute(
            "bob",
            &prepare(
                &authority,
                phone.clone(),
                command_id,
                b"bob-flow",
                (3, 1, 1),
                target("server", 31, SourceResidency::Server),
            ),
        )
        .unwrap();
    assert_ne!(actor_rejection.code, 0);
    assert_eq!(store.decision("alice", &phone, command_id).unwrap(), a);
    assert_eq!(store.decision("alice", &tablet, command_id).unwrap(), b);
    assert_eq!(
        store.decision("bob", &phone, command_id).unwrap(),
        actor_rejection
    );

    a_wrong_authority_reserves_no_command_id(&store, &authority);
}

fn a_wrong_authority_reserves_no_command_id(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
) {
    let mut wrong_authority = prepare(
        authority,
        key(b"other"),
        b"wrong-authority",
        b"flow",
        (3, 1, 0),
        target("server", 41, SourceResidency::Server),
    );
    wrong_authority.authority = Some(identity(99));
    assert_eq!(
        store.execute("alice", &wrong_authority).unwrap_err().code(),
        Code::FailedPrecondition
    );
    let corrected = prepare(
        authority,
        key(b"other"),
        b"wrong-authority",
        b"flow",
        (3, 1, 0),
        target("server", 41, SourceResidency::Server),
    );
    assert_eq!(store.execute("alice", &corrected).unwrap().code, 0);
}

#[test]
fn durable_rejections_consume_capacity_and_changed_retries_never_replace_them() {
    let dir = Directory::new("decisions");
    let constrained = SourceAuthorityLimits {
        max_decisions: 1,
        ..limits()
    };
    let (store, authority) = create(&dir, constrained);
    let owner = key(b"owner");
    let rejected_command = prepare(
        &authority,
        owner.clone(),
        b"stale",
        b"flow",
        (99, 1, 0),
        target("server", 9, SourceResidency::Server),
    );
    let rejected = store.execute("alice", &rejected_command).unwrap();
    assert_ne!(rejected.code, 0);
    assert_eq!(rejected.control_revision, 1);
    assert_eq!(store.execute("alice", &rejected_command).unwrap(), rejected);

    let mut changed = rejected_command.clone();
    changed.expected_control_revision = 1;
    assert_eq!(
        store.execute("alice", &changed).unwrap_err().code(),
        Code::FailedPrecondition
    );
    let full = store
        .execute(
            "alice",
            &prepare(
                &authority,
                owner,
                b"fresh",
                b"fresh-flow",
                (1, 1, 0),
                target("server", 10, SourceResidency::Server),
            ),
        )
        .unwrap_err();
    assert_eq!(full.code(), Code::ResourceExhausted);
    assert!(store.decision("alice", &key(b"owner"), b"fresh").is_err());
}

#[test]
fn transition_refuses_owner_state_from_another_logical_key() {
    let dir = Directory::new("transition-owner-key");
    let (store, authority) = create(&dir, limits());
    let owner_b = key(b"owner-b");
    let workflow = b"owner-b-workflow";
    let prepared = store
        .execute(
            "alice",
            &prepare(
                &authority,
                owner_b,
                b"prepare-b",
                workflow,
                (1, 1, 0),
                target("server-b", 131, SourceResidency::Server),
            ),
        )
        .unwrap()
        .owner
        .unwrap();
    let owner_a = key(b"owner-a");
    let command = cancel(
        &authority,
        owner_a.clone(),
        b"cancel-a",
        workflow,
        2,
        1,
        prepared.ownership_generation,
    );
    let operation = contract::operation_key("alice", &owner_a, &command.command_id);
    let read = store.inner.database().begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let header =
        SourceAuthorityHeader::decode(meta.get("header").unwrap().unwrap().value()).unwrap();
    let committed_policy =
        AccessPolicy::decode(meta.get("policy").unwrap().unwrap().value()).unwrap();

    let error = transition::apply(
        &header,
        &committed_policy,
        Some(&prepared),
        &command,
        &operation,
        false,
    )
    .err()
    .expect("an owner from another logical key must be refused");
    assert_eq!(error.code(), Code::DataLoss);
}
