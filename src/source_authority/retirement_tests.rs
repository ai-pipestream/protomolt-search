use super::*;
use crate::test_support::ForkGuarded;
use crate::{
    control_plane::{ControlPolicy, DurableControlPlane, LegacyControlCheckpoint, StateWriteFault},
    pb::{
        storage::source_authority_command::Action, AccessAction, AccessPolicy, CollectionGrant,
        CollectionResource,
    },
};
use prost::Message;
use std::path::PathBuf;
use tonic::Code;

struct Directory(PathBuf);

impl Directory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "legacy-control-retirement-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn authority(&self) -> PathBuf {
        self.0.join("authority.redb")
    }

    fn legacy(&self) -> PathBuf {
        self.0.join("legacy.json")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

fn grant(principal: &str, collection: &str) -> CollectionGrant {
    CollectionGrant {
        principal: principal.into(),
        workspace: "workspace-a".into(),
        collection: collection.into(),
        actions: vec![AccessAction::Admin as i32],
        ..Default::default()
    }
}

fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 1,
        revision: 1,
        resources: ["books", "music"]
            .map(|collection| CollectionResource {
                workspace: "workspace-a".into(),
                collection: collection.into(),
            })
            .to_vec(),
        grants: vec![grant("alice", "books"), grant("alice", "music")],
    }
}

fn limits() -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 32,
        max_payload_bytes: 1 << 20,
        max_command_bytes: 64 << 10,
    }
}

fn key(collection: &str) -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: collection.into(),
        owner_id: Vec::new(),
    }
}

fn request(checkpoint: &[u8], command_id: &[u8]) -> LegacyControlRetirementRequest {
    LegacyControlRetirementRequest {
        format_version: 1,
        key: Some(key("books")),
        command_id: command_id.to_vec(),
        expected_checkpoint_sha256: crate::sha256::digest(checkpoint).to_vec(),
        expected_control_revision: 1,
        expected_policy_revision: 1,
    }
}

fn authority(dir: &Directory) -> (SourceAuthorityStore, SourceAuthorityIdentity) {
    let identity = identity(7);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &identity, &policy(), &limits()).unwrap();
    (store, identity)
}

fn legacy(dir: &Directory) -> DurableControlPlane {
    DurableControlPlane::open(
        dir.legacy(),
        ControlPolicy {
            lease_ms: 31_337,
            replication_factor: 3,
            split_rows: 123_456,
            merge_rows: 7_654,
            compact_segments: 11,
            compact_tombstone_ppm: 222_333,
            history_limit: 17,
        },
    )
    .unwrap()
    .with_collection("books")
    .unwrap()
}

fn replace_books_grants(
    identity: &SourceAuthorityIdentity,
    command_id: &[u8],
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(identity.clone()),
        key: Some(key("books")),
        command_id: command_id.to_vec(),
        expected_control_revision: 1,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: vec![grant("alice", "books")],
        })),
    }
}

#[test]
fn retirement_preserves_exact_checkpoint_and_retries_after_authority_revision_changes() {
    let dir = Directory::new("happy");
    let (authority, identity) = authority(&dir);
    let legacy = legacy(&dir);
    let checkpoint = legacy.checkpoint_for_import().unwrap();
    let request = request(&checkpoint, b"retire-books");

    let retired = authority
        .retire_legacy_control("alice", &request, &legacy)
        .unwrap();
    assert_eq!(retired.checkpoint_bytes(), checkpoint);
    let record = retired.record().clone();
    assert_eq!(record.format_version, 1);
    assert_eq!(record.authority.as_ref(), Some(&identity));
    assert_eq!(record.request.as_ref(), Some(&request));
    assert_eq!(record.checkpoint, checkpoint);
    let operation = record.operation.as_ref().unwrap();
    assert_eq!(operation.principal, "alice");
    assert_eq!(operation.key, request.key);
    assert_eq!(operation.command_id, request.command_id);
    let decoded = LegacyControlCheckpoint::decode(retired.checkpoint_bytes()).unwrap();
    assert_eq!(decoded.state().collection, "books");
    assert_eq!(decoded.policy().lease_ms, 31_337);
    assert_eq!(decoded.policy().replication_factor, 3);
    assert_eq!(decoded.policy().split_rows, 123_456);
    assert_eq!(decoded.policy().merge_rows, 7_654);
    assert_eq!(decoded.policy().compact_segments, 11);
    assert_eq!(decoded.policy().compact_tombstone_ppm, 222_333);
    assert_eq!(decoded.policy().history_limit, 17);
    assert_eq!(
        authority
            .policy("alice", "workspace-a", "books")
            .unwrap()
            .revision,
        1
    );
    assert_eq!(
        authority
            .decision("alice", request.key.as_ref().unwrap(), &request.command_id)
            .err()
            .unwrap()
            .code(),
        Code::NotFound
    );

    let clone = legacy.clone();
    for plane in [&legacy, &clone] {
        let error = plane
            .checkpoint_for_import()
            .err()
            .expect("retirement must close every clone");
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(error.message().contains("retired"), "{error}");
    }
    assert_eq!(
        authority
            .execute(
                "alice",
                &replace_books_grants(&identity, b"advance-authority")
            )
            .unwrap()
            .code,
        0
    );
    let replay = authority
        .retire_legacy_control("alice", &request, &legacy)
        .unwrap();
    assert_eq!(replay.record(), &record);
    assert_eq!(replay.checkpoint_bytes(), checkpoint);

    drop(replay);
    drop(retired);
    drop(clone);
    drop(legacy);
    let recovered = authority
        .recover_legacy_retirement("alice", &request, &dir.legacy())
        .unwrap();
    assert_eq!(recovered.record(), &record);
    assert_eq!(recovered.checkpoint_bytes(), checkpoint);
    drop(recovered);
    for open in [
        DurableControlPlane::open(dir.legacy(), ControlPolicy::default()),
        DurableControlPlane::open_existing(dir.legacy(), ControlPolicy::default()),
    ] {
        let error = open.err().unwrap();
        assert!(error.contains("retired"), "{error}");
    }
}

#[test]
fn invalid_retirement_admission_never_changes_the_legacy_authority() {
    let dir = Directory::new("admission");
    let (authority, _) = authority(&dir);
    let legacy = legacy(&dir);
    let checkpoint = legacy.checkpoint_for_import().unwrap();
    let original = std::fs::read(dir.legacy()).unwrap();
    let valid = request(&checkpoint, b"retire-books");

    let mut cases = Vec::new();
    let mut wrong = valid.clone();
    wrong.expected_checkpoint_sha256 = vec![9; 32];
    cases.push(("checkpoint", "alice", wrong, Code::FailedPrecondition));
    let mut wrong = valid.clone();
    wrong.key = Some(key("music"));
    cases.push(("resource", "alice", wrong, Code::FailedPrecondition));
    let mut wrong = valid.clone();
    wrong.expected_control_revision = 2;
    cases.push(("control revision", "alice", wrong, Code::FailedPrecondition));
    let mut wrong = valid.clone();
    wrong.expected_policy_revision = 2;
    cases.push(("policy revision", "alice", wrong, Code::FailedPrecondition));
    cases.push(("actor", "bob", valid, Code::PermissionDenied));

    for (name, principal, request, code) in cases {
        let error = authority
            .retire_legacy_control(principal, &request, &legacy)
            .err()
            .unwrap();
        assert_eq!(error.code(), code, "{name}: {error}");
        assert_eq!(std::fs::read(dir.legacy()).unwrap(), original, "{name}");
        assert_eq!(
            legacy.checkpoint_for_import().unwrap(),
            checkpoint,
            "{name}"
        );
    }
}

#[test]
fn retirement_requires_a_durable_legacy_authority() {
    let dir = Directory::new("memory");
    let (authority, _) = authority(&dir);
    let legacy = DurableControlPlane::in_memory(ControlPolicy::default());
    let checkpoint = legacy.checkpoint_for_import().unwrap();
    let error = authority
        .retire_legacy_control("alice", &request(&checkpoint, b"memory"), &legacy)
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("durable"), "{error}");
    assert_eq!(
        LegacyControlCheckpoint::decode(&legacy.checkpoint_for_import().unwrap())
            .unwrap()
            .state()
            .revision,
        1
    );
}

#[test]
fn retirement_write_faults_leave_the_new_authority_usable_and_recover_by_boundary() {
    for fault in [
        StateWriteFault::BeforeRename,
        StateWriteFault::AfterRename,
        StateWriteFault::AfterSync,
    ] {
        let dir = Directory::new(&format!("fault-{fault:?}"));
        let (authority, _) = authority(&dir);
        let legacy = legacy(&dir);
        let checkpoint = legacy.checkpoint_for_import().unwrap();
        let request = request(&checkpoint, b"faulted-retirement");
        legacy.arm_state_write_fault(fault);
        let error = authority
            .retire_legacy_control("alice", &request, &legacy)
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::Internal);
        assert_eq!(
            authority
                .policy("alice", "workspace-a", "books")
                .unwrap()
                .revision,
            1
        );

        if fault == StateWriteFault::BeforeRename {
            assert_eq!(legacy.checkpoint_for_import().unwrap(), checkpoint);
            let retired = authority
                .retire_legacy_control("alice", &request, &legacy)
                .unwrap();
            assert_eq!(retired.checkpoint_bytes(), checkpoint);
        } else {
            let closed = legacy.checkpoint_for_import().err().unwrap();
            assert_eq!(closed.code(), Code::FailedPrecondition);
            drop(legacy);
            let recovered = authority
                .recover_legacy_retirement("alice", &request, &dir.legacy())
                .unwrap();
            assert_eq!(recovered.checkpoint_bytes(), checkpoint);
        }
    }
}

#[test]
fn abrupt_retirement_worker() {
    let Some(mode) = std::env::var_os("PSEARCH_LEGACY_RETIREMENT_EXIT") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("PSEARCH_LEGACY_RETIREMENT_ROOT").unwrap());
    let dir = Directory(root);
    let (authority, _) = authority(&dir);
    let legacy = legacy(&dir);
    let checkpoint = legacy.checkpoint_for_import().unwrap();
    let request = request(&checkpoint, b"abrupt-retirement");
    legacy.arm_state_write_fault(match mode.to_str().unwrap() {
        "before" => StateWriteFault::ExitBeforeRename,
        "after" => StateWriteFault::ExitAfterRename,
        "synced" => StateWriteFault::ExitAfterSync,
        other => panic!("unknown retirement exit mode {other}"),
    });
    authority
        .retire_legacy_control("alice", &request, &legacy)
        .unwrap();
    panic!("retirement exit fault did not terminate worker");
}

#[test]
fn abrupt_retirement_boundaries_preserve_or_recover_the_exact_checkpoint() {
    for mode in ["before", "after", "synced"] {
        let dir = Directory::new(&format!("abrupt-{mode}"));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("source_authority::retirement_tests::abrupt_retirement_worker")
            .arg("--nocapture")
            .env("PSEARCH_LEGACY_RETIREMENT_EXIT", mode)
            .env("PSEARCH_LEGACY_RETIREMENT_ROOT", &dir.0)
            .status_guarded()
            .unwrap();
        assert_eq!(status.code(), Some(87));

        let identity = identity(7);
        let authority = SourceAuthorityStore::open(&dir.authority(), &identity).unwrap();
        let reference = DurableControlPlane::in_memory(ControlPolicy {
            lease_ms: 31_337,
            replication_factor: 3,
            split_rows: 123_456,
            merge_rows: 7_654,
            compact_segments: 11,
            compact_tombstone_ppm: 222_333,
            history_limit: 17,
        })
        .with_collection("books")
        .unwrap();
        let checkpoint = reference.checkpoint_for_import().unwrap();
        let request = request(&checkpoint, b"abrupt-retirement");
        if mode == "before" {
            let legacy = DurableControlPlane::open_existing(
                dir.legacy(),
                ControlPolicy {
                    lease_ms: 31_337,
                    replication_factor: 3,
                    split_rows: 123_456,
                    merge_rows: 7_654,
                    compact_segments: 11,
                    compact_tombstone_ppm: 222_333,
                    history_limit: 17,
                },
            )
            .unwrap();
            assert_eq!(legacy.checkpoint_for_import().unwrap(), checkpoint);
        } else {
            let recovered = authority
                .recover_legacy_retirement("alice", &request, &dir.legacy())
                .unwrap();
            assert_eq!(recovered.checkpoint_bytes(), checkpoint);
        }
    }
}

#[test]
fn recovery_refuses_corrupt_truncated_future_and_noncanonical_retirement_frames() {
    for damage in [
        "truncated",
        "future",
        "checksum",
        "unknown",
        "checkpoint",
        "oversized",
    ] {
        let dir = Directory::new(&format!("corrupt-{damage}"));
        let (authority, _) = authority(&dir);
        let legacy = legacy(&dir);
        let checkpoint = legacy.checkpoint_for_import().unwrap();
        let request = request(&checkpoint, b"corrupt-retirement");
        let retired = authority
            .retire_legacy_control("alice", &request, &legacy)
            .unwrap();
        drop(retired);
        drop(legacy);
        let mut bytes = std::fs::read(dir.legacy()).unwrap();
        match damage {
            "truncated" => bytes.truncate(20),
            "future" => {
                let mut record = LegacyControlRetirement::decode(&bytes[40..]).unwrap();
                record.format_version = 2;
                let payload = record.encode_to_vec();
                bytes.truncate(8);
                bytes.extend_from_slice(&crate::sha256::digest(&payload));
                bytes.extend_from_slice(&payload);
            }
            "checksum" => *bytes.last_mut().unwrap() ^= 1,
            "unknown" => {
                bytes.extend_from_slice(&[0x98, 0x06, 0x01]);
                let digest = crate::sha256::digest(&bytes[40..]);
                bytes[8..40].copy_from_slice(&digest);
            }
            "checkpoint" => {
                let mut record = LegacyControlRetirement::decode(&bytes[40..]).unwrap();
                record.checkpoint[0] ^= 1;
                let payload = record.encode_to_vec();
                bytes.truncate(8);
                bytes.extend_from_slice(&crate::sha256::digest(&payload));
                bytes.extend_from_slice(&payload);
            }
            "oversized" => bytes = vec![0; 17 * 1024 * 1024 + 1],
            _ => unreachable!(),
        }
        std::fs::write(dir.legacy(), &bytes).unwrap();
        let error = authority
            .recover_legacy_retirement("alice", &request, &dir.legacy())
            .err()
            .unwrap();
        assert_eq!(error.code(), Code::DataLoss, "{damage}: {error}");
    }
}

#[test]
fn recovery_requires_the_exact_request_and_current_admin() {
    let dir = Directory::new("recovery-request");
    let (authority, authority_identity) = authority(&dir);
    let legacy = legacy(&dir);
    let checkpoint = legacy.checkpoint_for_import().unwrap();
    let request = request(&checkpoint, b"recover-exact");
    let retired = authority
        .retire_legacy_control("alice", &request, &legacy)
        .unwrap();

    let locked = authority
        .recover_legacy_retirement("alice", &request, &dir.legacy())
        .err()
        .unwrap();
    assert_eq!(locked.code(), Code::FailedPrecondition);
    drop(retired);
    drop(legacy);

    let missing = dir.0.join("missing-retirement");
    let error = authority
        .recover_legacy_retirement("alice", &request, &missing)
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);

    let mut malformed = request.clone();
    malformed.key = None;
    let error = authority
        .recover_legacy_retirement("alice", &malformed, &dir.legacy())
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::InvalidArgument);

    let mut changed = request.clone();
    changed.expected_control_revision = 2;
    let error = authority
        .recover_legacy_retirement("alice", &changed, &dir.legacy())
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::AlreadyExists);
    let error = authority
        .recover_legacy_retirement("bob", &request, &dir.legacy())
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::PermissionDenied);

    let add_bob = SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority_identity.clone()),
        key: Some(key("books")),
        command_id: b"authorize-bob".to_vec(),
        expected_control_revision: 1,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: vec![grant("alice", "books"), grant("bob", "books")],
        })),
    };
    assert_eq!(authority.execute("alice", &add_bob).unwrap().code, 0);
    let error = authority
        .recover_legacy_retirement("bob", &request, &dir.legacy())
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);

    let other_dir = Directory::new("wrong-authority");
    let other_identity = identity(19);
    let other = SourceAuthorityStore::create(
        &other_dir.authority(),
        &other_identity,
        &policy(),
        &limits(),
    )
    .unwrap();
    let error = other
        .recover_legacy_retirement("alice", &request, &dir.legacy())
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);

    let revoke = SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority_identity),
        key: Some(key("books")),
        command_id: b"revoke-recovery".to_vec(),
        expected_control_revision: 2,
        expected_policy_revision: 2,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: Vec::new(),
        })),
    };
    assert_eq!(
        authority.execute("alice", &revoke).err().unwrap().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        authority
            .policy("alice", "workspace-a", "books")
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    let error = authority
        .recover_legacy_retirement("alice", &request, &dir.legacy())
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::PermissionDenied);
}
