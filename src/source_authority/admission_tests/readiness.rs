use super::*;

#[test]
fn readiness_needs_the_held_binding_and_is_terminal_for_the_generation() {
    let dir = Directory::new("readiness");
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &authority, &policy(false), &limits())
            .unwrap();
    let preparation = store
        .execute("alice", &prepare(&authority))
        .unwrap()
        .owner
        .unwrap();
    let verified =
        VerifiedOwnerCompletion::from_binding(&binding(&authority, &preparation, 5)).unwrap();
    let completion = verified.completion().clone();
    assert_eq!(completion.bound_at_sequence, 5);
    assert_eq!(completion.history_id, target().history_id);

    confirmations_without_the_held_binding_are_refused(
        &store,
        &authority,
        &preparation,
        (&verified, &completion),
    );
    let owner = readiness_commits_once_under_the_held_binding(&store, &authority, &verified);
    ready_is_terminal_for_the_generation(&store, &authority, &verified);
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(reopened.owner("alice", &key()).unwrap(), owner);

    // A READY row without its completion is corruption, not a default.
    {
        let tx = reopened.inner.database().begin_write().unwrap();
        {
            let mut owners = tx.open_table(OWNERS).unwrap();
            let mut stripped = owner.clone();
            stripped.readiness = None;
            owners
                .insert(
                    key().encode_to_vec().as_slice(),
                    stripped.encode_to_vec().as_slice(),
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

fn confirmations_without_the_held_binding_are_refused(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    preparation: &PreparedSourceOwner,
    (verified, completion): (&VerifiedOwnerCompletion, &SourceOwnerCompletion),
) {
    // The general path never confirms readiness, whatever the bytes say.
    assert_eq!(
        store
            .execute("alice", &confirm(authority, "c", 2, completion.clone()))
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    // Bytes that differ from the held binding's completion are not admitted.
    let mut altered = completion.clone();
    altered.bound_at_sequence = 6;
    assert_eq!(
        store
            .confirm_owner_ready("alice", &confirm(authority, "c", 2, altered), verified)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    // A binding of another preparation or another authority admits nothing.
    let mut other_preparation = preparation.clone();
    other_preparation.workflow_id = b"installation-two".to_vec();
    let foreign =
        VerifiedOwnerCompletion::from_binding(&binding(authority, &other_preparation, 5)).unwrap();
    assert_eq!(
        store
            .confirm_owner_ready(
                "alice",
                &confirm(authority, "c", 2, foreign.completion().clone()),
                &foreign
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let elsewhere =
        VerifiedOwnerCompletion::from_binding(&binding(&identity(9), preparation, 5)).unwrap();
    assert_eq!(
        store
            .confirm_owner_ready(
                "alice",
                &confirm(authority, "c", 2, completion.clone()),
                &elsewhere
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        VerifiedOwnerCompletion::from_binding(&SourceManagedBinding::default())
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
}

fn readiness_commits_once_under_the_held_binding(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    verified: &VerifiedOwnerCompletion,
) -> PreparedSourceOwner {
    let completion = verified.completion();
    // Another administrator cannot confirm with the actor's binding either:
    // the command is his, the decision is scoped to him, and it still needs
    // the same held binding — which is fine; the binding is the proof, not
    // the actor. The stale revision refuses first.
    let stale = store
        .confirm_owner_ready(
            "bob",
            &confirm(authority, "bob-stale", 1, completion.clone()),
            verified,
        )
        .unwrap();
    assert_eq!(stale.code, Code::FailedPrecondition as u32);
    assert_eq!(
        store.owner("alice", &key()).unwrap().phase,
        PreparedSourceOwnerPhase::Prepared as i32
    );

    let ready = store
        .confirm_owner_ready(
            "alice",
            &confirm(authority, "c", 2, completion.clone()),
            verified,
        )
        .unwrap();
    assert_eq!(ready.code, 0, "{}", ready.message);
    let owner = ready.owner.clone().unwrap();
    assert_eq!(owner.phase, PreparedSourceOwnerPhase::Ready as i32);
    assert_eq!(owner.ownership_generation, 1);
    assert_eq!(owner.control_revision, 3);
    assert_eq!(
        owner.readiness,
        Some(SourceOwnerReadiness {
            format_version: 1,
            completion: Some(completion.clone()),
        })
    );
    assert_eq!(store.owner("alice", &key()).unwrap(), owner);
    // Exact retry, with or without the holder, answers from the record.
    assert_eq!(
        store
            .confirm_owner_ready(
                "alice",
                &confirm(authority, "c", 2, completion.clone()),
                verified
            )
            .unwrap(),
        ready
    );
    assert_eq!(store.decision("alice", &key(), b"c").unwrap(), ready);
    owner
}

fn ready_is_terminal_for_the_generation(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    verified: &VerifiedOwnerCompletion,
) {
    let completion = verified.completion();
    // READY is terminal for this generation.
    let cancel = store
        .execute(
            "alice",
            &command(
                authority,
                "cancel",
                3,
                1,
                Action::Cancel(CancelPreparedSourceOwner {
                    workflow_id: b"installation-one".to_vec(),
                }),
            ),
        )
        .unwrap();
    assert_eq!(cancel.code, Code::FailedPrecondition as u32);
    let mut again = prepare(authority);
    again.command_id = b"prepare-again".to_vec();
    again.expected_control_revision = 3;
    again.expected_ownership_generation = 1;
    if let Some(Action::Prepare(request)) = again.action.as_mut() {
        request.workflow_id = b"installation-two".to_vec();
    }
    let again = store.execute("alice", &again).unwrap();
    assert_eq!(again.code, Code::FailedPrecondition as u32);
    // A new confirmation id under the same binding is refused at admission:
    // the binding's preparation is no longer the committed owner. Nothing is
    // recorded for it.
    assert_eq!(
        store
            .confirm_owner_ready(
                "alice",
                &confirm(authority, "c2", 3, completion.clone()),
                verified
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        store.decision("alice", &key(), b"c2").err().unwrap().code(),
        Code::NotFound
    );
}
