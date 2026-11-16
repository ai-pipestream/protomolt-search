use super::*;

#[test]
fn activation_commits_the_fence_only_from_ready_and_never_from_a_lease() {
    let dir = Directory::new("activation");
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &authority, &policy(false), &limits())
            .unwrap();
    let preparation = store
        .execute("alice", &prepare(&authority))
        .unwrap()
        .owner
        .unwrap();
    let (verified, active, owner) =
        activation_commits_only_from_ready(&store, &authority, &preparation);
    the_admission_sees_the_committed_fence(&store);
    // Replay on a fresh replica reproduces the fence; reopen validates it and
    // a tampered epoch refuses the open.
    let replica_dir = Directory::new("activation-replica");
    let replica = SourceAuthorityStore::create(
        &replica_dir.authority(),
        &authority,
        &policy(false),
        &limits(),
    )
    .unwrap();
    replica
        .replay_command("alice", &prepare(&authority))
        .unwrap();
    replica
        .replay_command("alice", &activate(&authority, "early", 2))
        .unwrap();
    replica
        .replay_command(
            "alice",
            &confirm(&authority, "c", 2, verified.completion().clone()),
        )
        .unwrap();
    assert_eq!(
        replica
            .replay_command("alice", &activate(&authority, "activate", 3))
            .unwrap(),
        active
    );
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(reopened.owner("alice", &key()).unwrap(), owner);
    {
        let tx = reopened.inner.database().begin_write().unwrap();
        {
            let mut owners = tx.open_table(OWNERS).unwrap();
            let mut forged = owner.clone();
            forged.activation.as_mut().unwrap().write_epoch = 2;
            owners
                .insert(
                    key().encode_to_vec().as_slice(),
                    forged.encode_to_vec().as_slice(),
                )
                .unwrap();
        }
        tx.commit().unwrap();
    }
    drop(reopened);
    assert_eq!(
        SourceAuthorityStore::open(&dir.authority(), &authority)
            .err()
            .unwrap()
            .code(),
        Code::DataLoss
    );
}

fn activation_commits_only_from_ready(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    preparation: &PreparedSourceOwner,
) -> (
    VerifiedOwnerCompletion,
    SourceAuthorityDecision,
    PreparedSourceOwner,
) {
    // PREPARED is not READY: recorded refusal, no fence.
    let early = store
        .execute("alice", &activate(authority, "early", 2))
        .unwrap();
    assert_eq!(early.code, Code::FailedPrecondition as u32);
    assert!(store.owner("alice", &key()).unwrap().activation.is_none());
    let verified =
        VerifiedOwnerCompletion::from_binding(&binding(authority, preparation, 1)).unwrap();
    let ready = store
        .confirm_owner_ready(
            "alice",
            &confirm(authority, "c", 2, verified.completion().clone()),
            &verified,
        )
        .unwrap();
    assert_eq!(ready.code, 0);
    // The target carries no lease; nothing but the committed READY fact and
    // a current Admin's activation allocates the epoch.
    let active = store
        .execute("alice", &activate(authority, "activate", 3))
        .unwrap();
    assert_eq!(active.code, 0, "{}", active.message);
    let owner = active.owner.clone().unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Active as i32);
    assert_eq!(
        owner.activation,
        Some(SourceOwnerActivation {
            format_version: 1,
            write_epoch: 1,
            activated_control_revision: 4,
        })
    );
    assert_eq!(
        store
            .execute("alice", &activate(authority, "activate", 3))
            .unwrap(),
        active
    );
    let twice = store
        .execute("alice", &activate(authority, "activate-2", 4))
        .unwrap();
    assert_eq!(twice.code, Code::FailedPrecondition as u32);
    (verified, active, owner)
}

/// The admission sees the fence; a stale epoch or a wrong action refuses.
fn the_admission_sees_the_committed_fence(store: &SourceAuthorityStore) {
    let admission = store.admission("alice").unwrap();
    assert_eq!(
        admission
            .admit_write(&key(), 1, AccessAction::Admin)
            .unwrap()
            .principal,
        "alice"
    );
    assert_eq!(
        admission
            .admit_write(&key(), 2, AccessAction::Admin)
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        admission
            .admit_write(&key(), 1, AccessAction::Ingest)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    drop(admission);
}
