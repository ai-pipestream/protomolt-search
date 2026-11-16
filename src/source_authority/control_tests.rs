use super::control::*;
use super::*;
use crate::pb::storage::{
    control_configuration::{Derived, Placement, Provider},
    source_authority_command::Action,
};
use crate::pb::{
    AccessAction, AccessPolicy, ClusterNodeState, CollectionGrant, CollectionResource,
    NodeCapacity, NodeResidency, PlacementActionKind, PlacementNode, PlacementTree,
    ShardReplicaRole,
};
use crate::test_support::ForkGuarded;
use redb::{ReadableTable, TableHandle};
use std::path::PathBuf;
use tonic::Code;

mod fixture;
mod install;
mod recovery;
mod review;

pub(crate) use fixture::{state, state_with_tree, tree};

pub(crate) struct Directory(pub(crate) PathBuf);

impl Directory {
    pub(crate) fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "control-install-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    pub(crate) fn authority(&self) -> PathBuf {
        self.0.join("authority.redb")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

pub(crate) fn grant(principal: &str, collection: &str) -> CollectionGrant {
    CollectionGrant {
        principal: principal.into(),
        workspace: "workspace-a".into(),
        collection: collection.into(),
        actions: vec![AccessAction::Admin as i32],
        ..Default::default()
    }
}

pub(crate) fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 3,
        revision: 1,
        resources: ["books", "music"]
            .map(|collection| CollectionResource {
                workspace: "workspace-a".into(),
                collection: collection.into(),
            })
            .to_vec(),
        grants: vec![
            grant("alice", "books"),
            grant("bob", "books"),
            grant("alice", "music"),
        ],
    }
}

pub(crate) fn limits(max_command_bytes: u32, max_decisions: u64) -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions,
        max_payload_bytes: 64 << 20,
        max_command_bytes,
    }
}

pub(crate) fn key(collection: &str) -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: collection.into(),
        owner_id: Vec::new(),
    }
}

pub(crate) fn store(
    dir: &Directory,
    seed: u8,
    limits: &SourceAuthorityLimits,
) -> SourceAuthorityStore {
    SourceAuthorityStore::create(&dir.authority(), &identity(seed), &policy(), limits).unwrap()
}

pub(crate) fn geometry() -> ControlProviderGeometry {
    ControlProviderGeometry {
        format_version: 1,
        backend_kind: "embedded-turbovec".into(),
        backend_version: "s21".into(),
        config_format: "turbovec-config-v1".into(),
        config_payload: vec![1, 2, 3],
        dimension: 384,
        bits_per_component: 4,
        row_bytes_formula_version: 1,
        scoring_fingerprint: "fp".into(),
    }
}

/// No placement, no derived columns, explicit provider geometry.
pub(crate) fn configuration() -> ControlConfiguration {
    ControlConfiguration {
        format_version: 1,
        placement: Some(Placement::NoPlacement(true)),
        route_codes: Vec::new(),
        derived: Some(Derived::NoDerived(true)),
        derived_fingerprint: String::new(),
        provider: Some(Provider::Geometry(geometry())),
        policy: Some(ControlPlannerPolicy {
            format_version: 1,
            planner_version: 1,
            reconcile: Some(ControlReconcilePolicy {
                lease_ms: 31_337,
                replication_factor: 3,
                split_rows: 123_456,
                merge_rows: 7_654,
                compact_segments: 11,
                compact_tombstone_ppm: 222_333,
                history_limit: 17,
            }),
        }),
    }
}

pub(crate) fn command(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control: u64,
    state: ControlInstallState,
) -> ControlInstallCommand {
    ControlInstallCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control,
        expected_policy_revision: 1,
        state: Some(state),
    }
}

/// Install `state` for books at `revision`; returns the receipt.
pub(crate) fn install(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    state: ControlInstallState,
    revision: u64,
) -> ControlInstallReceipt {
    let decision = store
        .install_control("alice", &command(authority, "install", revision, state))
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    decision.receipt.unwrap()
}

pub(crate) type TableRows = Vec<(Vec<u8>, Vec<u8>)>;

pub(crate) fn table_bytes(store: &SourceAuthorityStore) -> Vec<(String, TableRows)> {
    let tx = store.inner.database().begin_read().unwrap();
    let mut names: Vec<String> = tx
        .list_tables()
        .unwrap()
        .map(|t| t.name().to_string())
        .collect();
    names.sort();
    names
        .into_iter()
        .map(|name: String| {
            let rows = if name == META.name() {
                let table = tx.open_table(META).unwrap();
                table
                    .iter()
                    .unwrap()
                    .map(|e| {
                        let (k, v) = e.unwrap();
                        (k.value().as_bytes().to_vec(), v.value().to_vec())
                    })
                    .collect()
            } else {
                let definition = redb::TableDefinition::<&[u8], &[u8]>::new(name.as_str());
                let table = tx.open_table(definition).unwrap();
                table
                    .iter()
                    .unwrap()
                    .map(|e| {
                        let (k, v) = e.unwrap();
                        (k.value().to_vec(), v.value().to_vec())
                    })
                    .collect()
            };
            (name, rows)
        })
        .collect()
}

#[test]
fn install_exit_worker() {
    let Some(fault) = std::env::var_os("PSEARCH_CONTROL_INSTALL_EXIT_FAULT") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("PSEARCH_CONTROL_INSTALL_DIR").unwrap());
    let authority = identity(7);
    let store = SourceAuthorityStore::create(
        &root.join("authority.redb"),
        &authority,
        &policy(),
        &limits(64 << 10, 64),
    )
    .unwrap();
    *store.inner.fault.lock().unwrap() = Some(match fault.to_str().unwrap() {
        "before" => Fault::ExitBeforeCommit,
        "after" => Fault::ExitAfterCommit,
        other => panic!("unknown exit fault {other}"),
    });
    let _ = store.install_control("alice", &command(&authority, "install", 1, state(2)));
    panic!("exit fault did not terminate the worker");
}

#[test]
fn an_abrupt_exit_around_the_install_commit_recovers_and_converges() {
    for fault in ["before", "after"] {
        let dir = Directory::new(&format!("exit-{fault}"));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("source_authority::control_tests::install_exit_worker")
            .arg("--nocapture")
            .env("PSEARCH_CONTROL_INSTALL_EXIT_FAULT", fault)
            .env("PSEARCH_CONTROL_INSTALL_DIR", &dir.0)
            .status_guarded()
            .unwrap();
        assert_eq!(status.code(), Some(87), "{fault}");
        let authority = identity(7);
        let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
        let stored = reopened.control_install_decision("alice", &key("books"), b"install");
        if fault == "after" {
            assert_eq!(stored.unwrap().code, 0);
        } else {
            assert_eq!(stored.unwrap_err().code(), Code::NotFound);
        }
        // The retry converges either way: applied once, at revision 2.
        let decision = reopened
            .install_control("alice", &command(&authority, "install", 1, state(2)))
            .unwrap();
        assert_eq!(decision.code, 0, "{fault}: {}", decision.message);
        assert_eq!(decision.control_revision, 2);
        let snapshot = reopened.control_snapshot("alice", &key("books")).unwrap();
        drop(reopened);
        let again = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
        assert_eq!(
            again.control_snapshot("alice", &key("books")).unwrap(),
            snapshot
        );
    }
}
