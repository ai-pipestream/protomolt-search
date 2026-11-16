use super::*;

#[test]
fn binding_commit_faults_leave_control_usable_and_source_recoverable_by_format() {
    for (name, fault, code) in [
        ("before-commit", BindFault::BeforeCommit, Code::Aborted),
        ("after-commit", BindFault::AfterCommit, Code::Internal),
    ] {
        let mut fixture = fixture(name, false);
        let expected = SourceManagedBinding {
            format_version: 1,
            authority: Some(authority_identity(7)),
            preparation: Some(fixture.preparation.clone()),
            bound_at_sequence: 1,
        };
        fixture.catalog.bind_fault = Some(fault);
        let error = fixture
            .catalog
            .bind_prepared_owner(
                &fixture.authority.admission("alice").unwrap(),
                &fixture.authority,
                &fixture.preparation,
                1 << 20,
            )
            .err()
            .unwrap();
        assert_eq!(error.code(), code);
        assert_eq!(
            fixture.authority.owner("alice", &owner_key()).unwrap(),
            fixture.preparation
        );
        if matches!(fault, BindFault::BeforeCommit) {
            let controlled =
                AccessControlledCatalog::open(&fixture.dir.catalog(), &resource(), &fixture.admin)
                    .unwrap();
            assert_eq!(controlled.resource_binding(), &resource());
        } else {
            let managed = PreparedManagedCatalog::recover(
                &fixture.dir.catalog(),
                &fixture.authority,
                "alice",
                expected.authority.as_ref().unwrap(),
                &fixture.preparation,
            )
            .unwrap();
            assert_eq!(managed.binding("alice").unwrap(), expected);
            assert_managed_records(&managed, 1);
        }
    }
}

#[test]
fn abrupt_binding_exit_worker() {
    let Some(mode) = std::env::var_os("PSEARCH_MANAGED_BIND_EXIT") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("PSEARCH_MANAGED_BIND_ROOT").unwrap());
    let local_authorizer: Arc<dyn Authorizer> =
        Arc::new(PolicyAuthority::new(access_policy(false)).unwrap());
    let admin = AccessPermit::acquire(
        local_authorizer.clone(),
        "alice",
        "books",
        AccessAction::Admin,
    )
    .unwrap();
    let ingest =
        AccessPermit::acquire(local_authorizer, "alice", "books", AccessAction::Ingest).unwrap();
    let mut catalog =
        AccessControlledCatalog::create(&root.join("source.redb"), &resource(), &admin).unwrap();
    let receipt = catalog.accept(&ingest, &write()).unwrap();
    let identity = authority_identity(7);
    let authority = SourceAuthorityStore::create(
        &root.join("authority.redb"),
        &identity,
        &access_policy(false),
        &authority_limits(),
    )
    .unwrap();
    let preparation = authority
        .execute("alice", &prepare_command(&identity, receipt.history_id))
        .unwrap()
        .owner
        .unwrap();
    catalog.bind_fault = Some(match mode.to_str().unwrap() {
        "before" => BindFault::ExitBeforeCommit,
        "after" => BindFault::ExitAfterCommit,
        other => panic!("unknown managed binding exit mode {other}"),
    });
    catalog
        .bind_prepared_owner(
            &authority.admission("alice").unwrap(),
            &authority,
            &preparation,
            1 << 20,
        )
        .unwrap();
    panic!("binding exit fault did not terminate the worker");
}

#[test]
fn abrupt_binding_exit_recovers_format_eight_or_committed_format_nine() {
    for mode in ["before", "after"] {
        let dir = Directory::new(&format!("abrupt-{mode}"));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("document_catalog::managed::tests::binding_crash::abrupt_binding_exit_worker")
            .arg("--nocapture")
            .env("PSEARCH_MANAGED_BIND_EXIT", mode)
            .env("PSEARCH_MANAGED_BIND_ROOT", &dir.0)
            .status_guarded()
            .unwrap();
        assert_eq!(status.code(), Some(87));

        let identity = authority_identity(7);
        let authority = SourceAuthorityStore::open(&dir.authority(), &identity).unwrap();
        let preparation = authority.owner("alice", &owner_key()).unwrap();
        let local_authorizer: Arc<dyn Authorizer> =
            Arc::new(PolicyAuthority::new(access_policy(false)).unwrap());
        let admin =
            AccessPermit::acquire(local_authorizer, "alice", "books", AccessAction::Admin).unwrap();
        if mode == "before" {
            let controlled =
                AccessControlledCatalog::open(&dir.catalog(), &resource(), &admin).unwrap();
            assert_eq!(controlled.resource_binding(), &resource());
        } else {
            let managed = PreparedManagedCatalog::recover(
                &dir.catalog(),
                &authority,
                "alice",
                &identity,
                &preparation,
            )
            .unwrap();
            let binding = SourceManagedBinding {
                format_version: 1,
                authority: Some(identity),
                preparation: Some(preparation),
                bound_at_sequence: 1,
            };
            assert_eq!(managed.binding("alice").unwrap(), binding);
            assert_managed_records(&managed, 1);
        }
        assert_eq!(
            authority
                .policy("alice", "workspace-a", "books")
                .unwrap()
                .revision,
            1
        );
    }
}
