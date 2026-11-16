use super::*;

#[test]
fn configure_is_a_retained_control_command_over_applied_state() {
    let dir = Directory::new("configure-unapplied");
    let limits = limits(64 << 10, 64);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    // Without applied control state there is nothing to plan over.
    let refused = store
        .configure_capacity(
            "alice",
            &configure(&authority, "c1", 1, configuration("hot")),
        )
        .unwrap();
    assert_eq!(
        refused.code,
        Code::FailedPrecondition as u32,
        "{}",
        refused.message
    );
    assert!(
        refused.message.contains("no applied control state"),
        "{}",
        refused.message
    );
    drop(store);

    let fixture = installed("configure");
    let store = &fixture.store;
    let authority = &fixture.authority;
    let revision = fixture.revision;
    invalid_configurations_are_envelope_refusals(store, authority, revision);
    let accepted = configure_decisions_are_retained_per_actor(store, authority, revision);
    // The shared command_id namespace: an install id cannot be reused here
    // and vice versa.
    assert_eq!(
        store
            .configure_capacity(
                "alice",
                &configure(authority, "install", revision + 2, configuration("x"))
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        store
            .install_control(
                "alice",
                &install_command(authority, "c2", revision + 2, state(2)),
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let state = store.capacity_state("alice", &key("books")).unwrap();
    assert_eq!(state.configured_control_revision, revision + 2);
    assert_eq!(
        state.configuration.unwrap().policy.unwrap().tiers[0].name,
        "warm"
    );
    assert_eq!(
        store
            .capacity_configure_decision("alice", &key("books"), b"c2")
            .unwrap(),
        accepted
    );
    drop(fixture);
}

/// Invalid configurations are envelope refusals, never decisions.
fn invalid_configurations_are_envelope_refusals(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    revision: u64,
) {
    let mut device = configuration("hot");
    device.policy.as_mut().unwrap().tiers[0].residency = CapacityResidency::Device as i32;
    assert_eq!(
        store
            .configure_capacity("alice", &configure(authority, "bad-tier", revision, device))
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    let mut wide = configuration("hot");
    wide.max_total_records = MAX_CAPACITY_RECORDS + 1;
    assert_eq!(
        store
            .configure_capacity("alice", &configure(authority, "bad-bound", revision, wide))
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
}

fn configure_decisions_are_retained_per_actor(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    revision: u64,
) -> CapacityConfigureDecision {
    assert_eq!(
        store
            .configure_capacity(
                "bob",
                &configure(authority, "bob", revision, configuration("hot"))
            )
            .unwrap()
            .code,
        0
    );
    // bob's configuration landed; alice's exact retry of her own id is hers.
    let stale = store
        .configure_capacity(
            "alice",
            &configure(authority, "c1", revision, configuration("hot")),
        )
        .unwrap();
    assert_eq!(stale.code, Code::FailedPrecondition as u32);
    let accepted = store
        .configure_capacity(
            "alice",
            &configure(authority, "c2", revision + 1, configuration("warm")),
        )
        .unwrap();
    assert_eq!(accepted.code, 0, "{}", accepted.message);
    assert_eq!(accepted.control_revision, revision + 2);
    assert_eq!(
        store
            .configure_capacity(
                "alice",
                &configure(authority, "c2", revision + 1, configuration("warm"))
            )
            .unwrap(),
        accepted
    );
    assert_eq!(
        store
            .configure_capacity(
                "alice",
                &configure(authority, "c2", revision + 1, configuration("cold"))
            )
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    accepted
}
