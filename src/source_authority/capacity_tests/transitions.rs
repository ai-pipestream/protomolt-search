use super::*;

#[test]
fn transitions_register_report_expire_and_recover_identically() {
    let fixture = installed("transitions");
    let store = &fixture.store;
    let authority = &fixture.authority;
    let revision = fixture.revision;
    reports_need_a_configuration_and_a_registration(store, authority, revision);
    reports_land_retry_conflict_and_supersede(store, authority, revision);
    reports_against_uncommitted_identity_are_refused(store, authority);
    // Planner input is served from the committed rows; the planning instant
    // sits just after the observed window closes.
    let (rendered, digest, plan) = planner_view(store, 7 * COHORT_MS);
    assert!(rendered.contains("observation_epoch: 2"), "{rendered}");

    a_restart_supersedes_and_an_expiry_drops(store, authority, revision);
    let before = planner_view(store, 10 * COHORT_MS);
    assert_ne!(before.1, digest);
    assert_ne!(before.2, plan);

    // Reopen recomputes every count and re-lands every row; the planner
    // sees identical inputs and produces identical outputs.
    drop(fixture.store.clone());
    let Installed { dir, store, .. } = fixture;
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), authority).unwrap();
    let after = planner_view(&reopened, 10 * COHORT_MS);
    assert_eq!(after, before);
    let state = reopened.capacity_state("alice", &key("books")).unwrap();
    assert_eq!(
        (
            state.reporter_count,
            state.observation_count,
            state.observation_epoch
        ),
        (1, 1, 6)
    );
    assert_eq!(
        reopened
            .capacity_transition("alice", &report(authority, observation(2, 9, 2)))
            .unwrap()
            .outcome,
        CapacityTransitionOutcome::Unchanged as i32
    );
}

fn reports_need_a_configuration_and_a_registration(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    revision: u64,
) {
    assert_eq!(
        store
            .capacity_transition("alice", &register(authority, "node/α", 1))
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition,
        "no configuration yet"
    );
    assert_eq!(
        store
            .configure_capacity(
                "alice",
                &configure(authority, "cfg", revision, configuration("hot"))
            )
            .unwrap()
            .code,
        0
    );
    // A report before registration refuses with Kimi's refusal text.
    let error = store
        .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("has not registered"), "{error}");
    let registered = store
        .capacity_transition("alice", &register(authority, "node/α", 1))
        .unwrap();
    assert_eq!(registered.observation_epoch, 0);
    assert_eq!(
        store
            .capacity_transition("alice", &register(authority, "node/α", 1))
            .unwrap(),
        registered
    );
}

/// Land, exact retry, conflict, supersession, newer window.
fn reports_land_retry_conflict_and_supersede(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    revision: u64,
) {
    let landed = store
        .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
        .unwrap();
    assert_eq!(landed.outcome, CapacityTransitionOutcome::Landed as i32);
    assert_eq!(landed.observation_epoch, 1);
    assert_eq!(landed.control_revision, revision + 1);
    let again = store
        .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
        .unwrap();
    assert_eq!(again.outcome, CapacityTransitionOutcome::Unchanged as i32);
    assert_eq!(again.observation_epoch, 1);
    let conflict = store
        .capacity_transition("alice", &report(authority, observation(1, 5, 11)))
        .err()
        .unwrap();
    assert_eq!(conflict.code(), Code::FailedPrecondition);
    assert!(conflict.message().contains("conflicts"), "{conflict}");
    let older = store
        .capacity_transition("alice", &report(authority, observation(1, 4, 3)))
        .unwrap();
    assert_eq!(older.outcome, CapacityTransitionOutcome::Superseded as i32);
    assert_eq!(older.observation_epoch, 1);
    let newer = store
        .capacity_transition("alice", &report(authority, observation(1, 6, 12)))
        .unwrap();
    assert_eq!(newer.outcome, CapacityTransitionOutcome::Landed as i32);
    assert_eq!(newer.observation_epoch, 2);
    let state = store.capacity_state("alice", &key("books")).unwrap();
    assert_eq!((state.reporter_count, state.observation_count), (1, 1));
}

/// Reports against uncommitted identity are refused by name.
fn reports_against_uncommitted_identity_are_refused(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
) {
    let mut wrong_leaf = observation(1, 6, 12);
    wrong_leaf.leaf = "rest".into();
    let error = store
        .capacity_transition("alice", &report(authority, wrong_leaf))
        .err()
        .unwrap();
    assert!(
        error.message().contains("covers no rows in leaf rest"),
        "{error}"
    );
    let mut wrong_generation = observation(1, 6, 12);
    wrong_generation.shard.as_mut().unwrap().source_generation = 8;
    let error = store
        .capacity_transition("alice", &report(authority, wrong_generation))
        .err()
        .unwrap();
    assert!(
        error
            .message()
            .contains("source generation 8 is not the committed 7"),
        "{error}"
    );
    let mut misaligned = observation(1, 6, 12);
    misaligned.window_start_unix_ms += 1;
    assert_eq!(
        store
            .capacity_transition("alice", &report(authority, misaligned))
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
}

fn a_restart_supersedes_and_an_expiry_drops(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    revision: u64,
) {
    // A restart of node/α: the new incarnation supersedes the old one and
    // its observations are dropped in the same committed transition.
    let restarted = store
        .capacity_transition("alice", &register(authority, "node/α", 2))
        .unwrap();
    assert_eq!(restarted.dropped, 1);
    assert_eq!(restarted.observation_epoch, 3);
    let error = store
        .capacity_transition("alice", &report(authority, observation(1, 7, 1)))
        .err()
        .unwrap();
    assert!(error.message().contains("is superseded by"), "{error}");
    assert_eq!(
        store
            .capacity_transition("alice", &report(authority, observation(2, 7, 1)))
            .unwrap()
            .observation_epoch,
        4
    );
    // The cohort cannot change with observations retained; expiry first.
    let mut shifted = configuration("hot");
    shifted.cohort_phase_unix_ms = 5;
    let refused = store
        .configure_capacity(
            "alice",
            &configure(authority, "shift", revision + 1, shifted),
        )
        .unwrap();
    assert_eq!(
        refused.code,
        Code::FailedPrecondition as u32,
        "{}",
        refused.message
    );
    let nothing = store
        .capacity_transition("alice", &expire(authority, 8 * COHORT_MS, 10 * COHORT_MS))
        .unwrap();
    assert_eq!((nothing.dropped, nothing.observation_epoch), (0, 4));
    let expired = store
        .capacity_transition("alice", &expire(authority, 20 * COHORT_MS, COHORT_MS))
        .unwrap();
    assert_eq!((expired.dropped, expired.observation_epoch), (1, 5));
    let state = store.capacity_state("alice", &key("books")).unwrap();
    assert_eq!(
        (
            state.reporter_count,
            state.observation_count,
            state.observation_epoch
        ),
        (1, 0, 5)
    );
    assert_eq!(
        store
            .capacity_transition("alice", &report(authority, observation(2, 9, 2)))
            .unwrap()
            .observation_epoch,
        6
    );
}

#[test]
fn policy_and_configuration_changes_are_bound_into_the_plan_digest() {
    let fixture = installed("policy-changes");
    let store = &fixture.store;
    let authority = &fixture.authority;
    let revision = fixture.revision;
    assert_eq!(
        store
            .configure_capacity(
                "alice",
                &configure(authority, "cfg", revision, configuration("hot"))
            )
            .unwrap()
            .code,
        0
    );
    store
        .capacity_transition("alice", &register(authority, "node/α", 1))
        .unwrap();
    store
        .capacity_transition("alice", &report(authority, observation(1, 5, 10)))
        .unwrap();
    let base = planner_view(store, 6 * COHORT_MS);
    // An access-policy change advances the policy revision the plan binds.
    let grants = widened_grants(authority, revision);
    assert_eq!(store.execute("alice", &grants).unwrap().code, 0);
    let after_grants = planner_view(store, 6 * COHORT_MS);
    assert_ne!(after_grants.1, base.1);
    assert!(
        after_grants.0.contains("policy_revision: 2"),
        "{}",
        after_grants.0
    );
    // A tier-policy change is a control command and changes the digest too.
    let mut cfg = configure(authority, "cfg2", revision + 2, configuration("warm"));
    cfg.expected_policy_revision = 2;
    assert_eq!(store.configure_capacity("alice", &cfg).unwrap().code, 0);
    let after_policy = planner_view(store, 6 * COHORT_MS);
    assert_ne!(after_policy.1, after_grants.1);
    // Observations survive both; a revoked actor cannot read the input.
    assert!(
        after_policy.0.contains("observation_epoch: 1"),
        "{}",
        after_policy.0
    );
    assert_eq!(
        store
            .planner_input("carol", &key("books"), &context(6 * COHORT_MS))
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    // Reopen reproduces the final view exactly.
    let Installed { dir, store, .. } = fixture;
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), authority).unwrap();
    assert_eq!(planner_view(&reopened, 6 * COHORT_MS), after_policy);
}

/// A grant replacement that keeps both administrators and lets alice search.
fn widened_grants(authority: &SourceAuthorityIdentity, revision: u64) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        command_id: b"grants".to_vec(),
        expected_control_revision: revision + 1,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: vec![
                CollectionGrant {
                    principal: "alice".into(),
                    workspace: "workspace-a".into(),
                    collection: "books".into(),
                    actions: vec![AccessAction::Admin as i32, AccessAction::Search as i32],
                    ..Default::default()
                },
                CollectionGrant {
                    principal: "bob".into(),
                    workspace: "workspace-a".into(),
                    collection: "books".into(),
                    actions: vec![AccessAction::Admin as i32],
                    ..Default::default()
                },
            ],
        })),
    }
}
