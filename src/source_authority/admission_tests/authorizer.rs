use super::*;

#[test]
fn the_store_is_a_workspace_authorizer_with_one_ordered_policy_history() {
    let dir = Directory::new("authorizer");
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &authority, &policy(true), &limits())
            .unwrap();
    let shared: Arc<dyn Authorizer> = Arc::new(store.clone());
    assert_eq!(*shared.subscribe().borrow(), 1);
    let permit =
        AccessPermit::acquire(shared.clone(), "alice", "books", AccessAction::Ingest).unwrap();
    assert_eq!(permit.decision().policy_revision, 1);
    assert_eq!(permit.decision().workspace, "workspace-a");
    assert_eq!(
        AccessPermit::acquire(shared.clone(), "carol", "books", AccessAction::Search)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        AccessPermit::acquire(shared.clone(), "alice", "music", AccessAction::Search)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );

    a_revocation_waits_for_the_admitted_operation(&store, &authority, &shared, &permit);
    assert_eq!(
        AccessPermit::acquire(shared.clone(), "alice", "books", AccessAction::Ingest)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    let admin =
        AccessPermit::acquire(shared.clone(), "alice", "books", AccessAction::Admin).unwrap();
    assert_eq!(admin.decision().policy_revision, 2);
    drop(admin.pin().unwrap());

    // Reopen publishes the committed revision, not a default.
    drop(permit);
    drop(admin);
    drop(shared);
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(*reopened.subscribe().borrow(), 2);
    assert_eq!(
        reopened
            .authorize("alice", "books", AccessAction::Admin)
            .unwrap()
            .policy_revision,
        2
    );
    assert_eq!(
        reopened.admission("").err().unwrap().code(),
        Code::InvalidArgument
    );
}

/// An admitted operation holds the fence: a revocation committed by another
/// administrator waits for it and publishes the new revision afterwards.
fn a_revocation_waits_for_the_admitted_operation(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    shared: &Arc<dyn Authorizer>,
    permit: &AccessPermit,
) {
    let pin = permit.pin().unwrap();
    assert_eq!(pin.decision(), permit.decision());
    let (result_tx, result_rx) = mpsc::channel();
    let revoking = {
        let store = store.clone();
        let authority = authority.clone();
        std::thread::spawn(move || {
            let revoke = grants_command(
                &authority,
                "bob-revokes-ingest",
                1,
                1,
                vec![
                    grant("alice", &[AccessAction::Admin]),
                    grant("bob", &[AccessAction::Admin]),
                ],
            );
            result_tx.send(store.execute("bob", &revoke)).unwrap();
        })
    };
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        matches!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "revocation completed while an operation remained admitted"
    );
    assert_eq!(*shared.subscribe().borrow(), 1);
    drop(pin);
    let decision = result_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    revoking.join().unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    assert_eq!(decision.policy_revision, 2);
    assert_eq!(*shared.subscribe().borrow(), 2);
    assert_eq!(permit.check().err().unwrap().code(), Code::PermissionDenied);
    assert_eq!(permit.pin().err().unwrap().code(), Code::PermissionDenied);
}
