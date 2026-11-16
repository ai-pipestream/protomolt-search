use super::*;

#[test]
fn commit_faults_distinguish_known_abort_from_ambiguous_shared_failure() {
    let dir = Directory::new("faults");
    let (store, authority) = create(&dir, limits());
    let before = prepare(
        &authority,
        key(b"before"),
        b"before",
        b"before-flow",
        (1, 1, 0),
        target("server", 81, SourceResidency::Server),
    );
    *store.inner.fault.lock().unwrap() = Some(Fault::BeforeCommit);
    let aborted = store.execute("alice", &before).unwrap_err();
    assert_eq!(aborted.code(), Code::Aborted);
    assert!(store.owner("alice", &key(b"before")).is_err());
    let accepted = store.execute("alice", &before).unwrap();
    assert_eq!(accepted.code, 0);

    let after = prepare(
        &authority,
        key(b"after"),
        b"after",
        b"after-flow",
        (2, 1, 0),
        target("server", 91, SourceResidency::Server),
    );
    let clone = store.clone();
    *store.inner.fault.lock().unwrap() = Some(Fault::AfterCommit);
    let ambiguous = store.execute("alice", &after).unwrap_err();
    assert_eq!(ambiguous.code(), Code::Internal);
    for error in [
        store.owner("alice", &key(b"after")).unwrap_err(),
        clone.execute("alice", &after).unwrap_err(),
        clone
            .decision("alice", &key(b"after"), b"after")
            .unwrap_err(),
    ] {
        assert_eq!(error.code(), Code::FailedPrecondition);
    }

    drop(clone);
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.store(), &authority).unwrap();
    let committed = reopened
        .decision("alice", &key(b"after"), b"after")
        .unwrap();
    assert_eq!(committed.code, 0);
    assert_eq!(committed.control_revision, 3);
    assert_eq!(
        reopened.owner("alice", &key(b"after")).unwrap(),
        committed.owner.unwrap()
    );
}

#[derive(Clone, Copy)]
enum Damage {
    HeaderCount,
    OwnerLink,
    WorkflowLink,
}

#[test]
fn reopen_audits_header_counts_and_owner_workflow_links() {
    for damage in [Damage::HeaderCount, Damage::OwnerLink, Damage::WorkflowLink] {
        let dir = Directory::new(match damage {
            Damage::HeaderCount => "damage-count",
            Damage::OwnerLink => "damage-owner",
            Damage::WorkflowLink => "damage-workflow",
        });
        let (store, authority) = create(&dir, limits());
        let command = prepare(
            &authority,
            key(b"linked-owner"),
            b"linked-command",
            b"linked-workflow",
            (1, 1, 0),
            target("server", 121, SourceResidency::Server),
        );
        assert_eq!(store.execute("alice", &command).unwrap().code, 0);
        drop(store);

        let database = redb::Database::open(dir.store()).unwrap();
        let write = database.begin_write().unwrap();
        match damage {
            Damage::HeaderCount => {
                let mut meta = write.open_table(META).unwrap();
                let bytes = meta.get("header").unwrap().unwrap().value().to_vec();
                let mut header = SourceAuthorityHeader::decode(bytes.as_slice()).unwrap();
                header.owner_count += 1;
                meta.insert("header", header.encode_to_vec().as_slice())
                    .unwrap();
            }
            Damage::OwnerLink => {
                let mut owners = write.open_table(OWNERS).unwrap();
                let (key_bytes, mut owner) = {
                    let (key_bytes, value_bytes) = owners.iter().unwrap().next().unwrap().unwrap();
                    (
                        key_bytes.value().to_vec(),
                        PreparedSourceOwner::decode(value_bytes.value()).unwrap(),
                    )
                };
                owner.last_command.as_mut().unwrap().command_id = b"absent-command".to_vec();
                owners
                    .insert(key_bytes.as_slice(), owner.encode_to_vec().as_slice())
                    .unwrap();
            }
            Damage::WorkflowLink => {
                let mut workflows = write.open_table(WORKFLOWS).unwrap();
                let (key_bytes, mut workflow) = {
                    let (key_bytes, value_bytes) =
                        workflows.iter().unwrap().next().unwrap().unwrap();
                    (
                        key_bytes.value().to_vec(),
                        SourceAuthorityWorkflow::decode(value_bytes.value()).unwrap(),
                    )
                };
                workflow.preparation_command.as_mut().unwrap().command_id =
                    b"absent-command".to_vec();
                workflows
                    .insert(key_bytes.as_slice(), workflow.encode_to_vec().as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        drop(database);

        let error = SourceAuthorityStore::open(&dir.store(), &authority)
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::DataLoss);
    }
}

fn committed_state(
    store: &SourceAuthorityStore,
) -> (SourceAuthorityHeader, AccessPolicy, u64, u64, u64) {
    let read = store.inner.database().begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let header =
        SourceAuthorityHeader::decode(meta.get("header").unwrap().unwrap().value()).unwrap();
    let policy = AccessPolicy::decode(meta.get("policy").unwrap().unwrap().value()).unwrap();
    let owners = read.open_table(OWNERS).unwrap().len().unwrap();
    let decisions = read.open_table(DECISIONS).unwrap().len().unwrap();
    let workflows = read.open_table(WORKFLOWS).unwrap().len().unwrap();
    (header, policy, owners, decisions, workflows)
}

#[test]
fn capacity_refusals_abort_the_whole_command_transaction() {
    let authority = identity(7);
    let first = prepare(
        &authority,
        key(b"first-owner"),
        b"first-command",
        b"first-workflow",
        (1, 1, 0),
        target("server-a", 141, SourceResidency::Server),
    );
    let second = prepare(
        &authority,
        key(b"second-owner"),
        b"second-command",
        b"second-workflow",
        (2, 1, 0),
        target("server-b", 151, SourceResidency::Server),
    );

    let measurement = Directory::new("capacity-measurement");
    let command_limit = first.encoded_len().max(second.encoded_len()) as u32;
    let measurement_limits = SourceAuthorityLimits {
        max_command_bytes: command_limit,
        ..limits()
    };
    let measured = SourceAuthorityStore::create(
        &measurement.store(),
        &authority,
        &policy(1),
        &measurement_limits,
    )
    .unwrap();
    assert_eq!(measured.execute("alice", &first).unwrap().code, 0);
    let first_payload = committed_state(&measured).0.payload_bytes;
    drop(measured);

    for (name, constrained) in [
        (
            "owner-capacity",
            SourceAuthorityLimits {
                max_owners: 1,
                max_command_bytes: command_limit,
                ..limits()
            },
        ),
        (
            "payload-capacity",
            SourceAuthorityLimits {
                max_payload_bytes: first_payload,
                max_command_bytes: command_limit,
                ..limits()
            },
        ),
    ] {
        a_capacity_refusal_changes_nothing(name, &constrained, (&first, &second));
    }
}

fn a_capacity_refusal_changes_nothing(
    name: &str,
    constrained: &SourceAuthorityLimits,
    (first, second): (&SourceAuthorityCommand, &SourceAuthorityCommand),
) {
    let authority = identity(7);
    let dir = Directory::new(name);
    let store =
        SourceAuthorityStore::create(&dir.store(), &authority, &policy(1), constrained).unwrap();
    let original = store.execute("alice", first).unwrap();
    assert_eq!(original.code, 0);
    let before = committed_state(&store);

    let error = store.execute("alice", second).unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(committed_state(&store), before);
    assert_eq!(
        store
            .owner("alice", second.key.as_ref().unwrap())
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        store
            .decision("alice", second.key.as_ref().unwrap(), &second.command_id)
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(store.execute("alice", first).unwrap(), original);
}
