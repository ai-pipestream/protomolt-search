use super::import::*;
use super::*;
use crate::control_plane::{
    test_fixtures, DurableControlPlane, LegacyControlCheckpoint, RetiredLegacyControl,
};
use crate::pb::storage::{
    control_import_command::Action as ImportAction,
    legacy_control_import_supplement::{Derived, Placement, Provider},
    source_authority_command::Action,
};
use crate::pb::{
    AccessAction, AccessPolicy, CollectionGrant, CollectionResource, DerivedColumn, DerivedColumns,
    DerivedDisclosure, MaterializeKind, PlacementNode, PlacementTree,
};
use crate::test_support::ForkGuarded;
use redb::{ReadableTable, TableHandle};
use std::path::PathBuf;
use tonic::Code;

pub(super) struct Directory(pub(super) PathBuf);

impl Directory {
    pub(super) fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "control-import-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    pub(super) fn authority(&self) -> PathBuf {
        self.0.join("authority.redb")
    }
    pub(super) fn legacy(&self) -> PathBuf {
        self.0.join("legacy.json")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

pub(super) fn grant(principal: &str, collection: &str) -> CollectionGrant {
    CollectionGrant {
        principal: principal.into(),
        workspace: "workspace-a".into(),
        collection: collection.into(),
        actions: vec![AccessAction::Admin as i32],
        ..Default::default()
    }
}

pub(super) fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 1,
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

pub(super) fn limits(max_command_bytes: u32, max_decisions: u64) -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions,
        max_payload_bytes: 64 << 20,
        max_command_bytes,
    }
}

pub(super) fn key(collection: &str) -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: "workspace-a".into(),
        collection: collection.into(),
        owner_id: Vec::new(),
    }
}

/// A populated legacy authority with `routes` current routes, opened durably.
pub(super) fn legacy(dir: &Directory, routes: usize) -> DurableControlPlane {
    std::fs::write(
        dir.legacy(),
        test_fixtures::populated_state_json("books", routes),
    )
    .unwrap();
    DurableControlPlane::open_existing(dir.legacy(), test_fixtures::populated_policy())
        .unwrap()
        .with_collection("books")
        .unwrap()
}

pub(super) fn store(
    dir: &Directory,
    seed: u8,
    limits: &SourceAuthorityLimits,
) -> SourceAuthorityStore {
    SourceAuthorityStore::create(&dir.authority(), &identity(seed), &policy(), limits).unwrap()
}

pub(super) fn retire(
    store: &SourceAuthorityStore,
    legacy: &DurableControlPlane,
    control_revision: u64,
) -> (RetiredLegacyControl, LegacyControlRetirementRequest) {
    let checkpoint = legacy.checkpoint_for_import().unwrap();
    let request = LegacyControlRetirementRequest {
        format_version: 1,
        key: Some(key("books")),
        command_id: b"retire-books".to_vec(),
        expected_checkpoint_sha256: crate::sha256::digest(&checkpoint).to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: 1,
    };
    let retired = store
        .retire_legacy_control("alice", &request, legacy)
        .unwrap();
    (retired, request)
}

pub(super) fn geometry() -> ControlProviderGeometry {
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

pub(super) fn supplement(checkpoint: &LegacyControlCheckpoint) -> LegacyControlImportSupplement {
    LegacyControlImportSupplement {
        format_version: 1,
        placement: Some(Placement::NoPlacement(true)),
        route_codes: Vec::new(),
        derived: Some(Derived::NoDerived(true)),
        derived_fingerprint: String::new(),
        provider: Some(Provider::Geometry(geometry())),
        policy: Some(ControlPlannerPolicy {
            format_version: 1,
            planner_version: 1,
            control: Some(*checkpoint.policy()),
        }),
        history_placement_unavailable: true,
    }
}

pub(super) fn payload(
    retired: &RetiredLegacyControl,
    supplement: LegacyControlImportSupplement,
) -> Vec<u8> {
    ControlImportPayload {
        format_version: 1,
        retirement: retired.record().encode_to_vec(),
        supplement: Some(supplement),
    }
    .encode_to_vec()
}

pub(super) fn checkpoint_of(retired: &RetiredLegacyControl) -> LegacyControlCheckpoint {
    LegacyControlCheckpoint::decode(retired.checkpoint_bytes()).unwrap()
}

pub(super) fn command(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control: u64,
    action: ImportAction,
) -> ControlImportCommand {
    ControlImportCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control,
        expected_policy_revision: 1,
        workflow_id: b"import-1".to_vec(),
        action: Some(action),
    }
}

pub(super) fn begin_action(
    retired: &RetiredLegacyControl,
    payload: &[u8],
    chunk_bytes: u32,
    chunk_count: u32,
) -> ImportAction {
    ImportAction::Begin(BeginControlImport {
        retirement_sha256: retirement_digest(retired.record()),
        retirement_operation: retired.record().operation.clone(),
        payload_bytes: payload.len() as u64,
        payload_sha256: payload_digest(payload),
        chunk_bytes,
        chunk_count,
    })
}

pub(super) fn chunk_action(payload: &[u8], chunk_bytes: u32, ordinal: u32) -> ImportAction {
    let start = ordinal as usize * chunk_bytes as usize;
    let end = payload.len().min(start + chunk_bytes as usize);
    ImportAction::Chunk(ControlImportChunk {
        ordinal,
        bytes: payload[start..end].to_vec(),
        sha256: chunk_digest(&payload[start..end]),
    })
}

/// Begin, every chunk, Commit; returns the receipt and the next revision.
pub(super) fn run_import(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    limits: &SourceAuthorityLimits,
    retired: &RetiredLegacyControl,
    payload: &[u8],
    mut revision: u64,
) -> (ControlImportReceipt, u64, u32) {
    let (chunk_bytes, chunk_count) =
        plan_chunks(limits, authority, &key("books"), payload).unwrap();
    let begin = store
        .begin_control_import(
            "alice",
            &command(
                authority,
                "begin",
                revision,
                begin_action(retired, payload, chunk_bytes, chunk_count),
            ),
            retired,
        )
        .unwrap();
    assert_eq!(begin.code, 0, "{}", begin.message);
    revision += 1;
    for ordinal in 0..chunk_count {
        let decision = store
            .execute_control_import(
                "alice",
                &command(
                    authority,
                    &format!("chunk-{ordinal}"),
                    revision,
                    chunk_action(payload, chunk_bytes, ordinal),
                ),
            )
            .unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
        revision += 1;
    }
    let commit = store
        .execute_control_import(
            "alice",
            &command(
                authority,
                "commit",
                revision,
                ImportAction::Commit(CommitControlImport {}),
            ),
        )
        .unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
    (commit.receipt.unwrap(), revision + 1, chunk_count)
}

pub(super) type TableRows = Vec<(Vec<u8>, Vec<u8>)>;

pub(super) fn table_bytes(store: &SourceAuthorityStore) -> Vec<(String, TableRows)> {
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
fn round_trip_imports_the_populated_legacy_authority_in_bounded_chunks() {
    let dir = Directory::new("round-trip");
    let limits = limits(8 << 10, 64);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    let legacy = legacy(&dir, 300);
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let payload = payload(&retired, supplement(&checkpoint));
    let capacity = chunk_capacity(&limits, &authority, &key("books")).unwrap();
    assert!(
        payload.len() > 2 * capacity,
        "the fixture must need several chunks"
    );

    let (receipt, next, chunk_count) =
        run_import(&store, &authority, &limits, &retired, &payload, 1);
    assert!(chunk_count >= 3);
    let state = checkpoint.state();
    assert_eq!(receipt.routes, 300);
    assert_eq!(receipt.history, 2);
    assert_eq!(receipt.nodes, 2);
    assert_eq!(receipt.replicas, 2);
    assert_eq!(receipt.actions, 1);
    assert_eq!(receipt.completed_actions, 2);
    assert_eq!(receipt.imported_legacy_revision, state.revision);
    assert_eq!(receipt.imported_generation, 3);
    assert_eq!(receipt.control_revision, next);

    let snapshot = store.control_snapshot("alice", &key("books")).unwrap();
    let applied = snapshot.state.as_ref().unwrap();
    assert_eq!(applied.next_token, u64::MAX);
    assert_eq!(applied.next_action, u64::MAX);
    assert_eq!(applied.imported_legacy_revision, u64::MAX);
    assert_eq!(applied.history_generations, [1, 2]);
    assert!(!applied.history_rollback_available);
    assert_eq!(
        applied.configuration.as_ref().unwrap().provider,
        Some(Provider::Geometry(geometry()))
    );
    let topology = snapshot.topology.as_ref().unwrap();
    assert_eq!(topology.routes, state.topology.as_ref().unwrap().routes);
    assert!(topology.current && !topology.codes_available);
    assert_eq!(snapshot.nodes.len(), 2);
    // Residency is the legacy node's own declaration, copied exactly.
    let residency: Vec<(String, i32)> = snapshot
        .nodes
        .iter()
        .map(|n| (n.node_id.clone(), n.residency))
        .collect();
    assert_eq!(
        residency,
        [
            ("node z".to_string(), SourceResidency::DeviceLocal as i32),
            ("node/α".to_string(), SourceResidency::Server as i32),
        ]
    );
    assert_eq!(snapshot.replicas.len(), 2);
    assert_eq!(snapshot.actions.len(), 1);
    assert_eq!(
        snapshot.actions[0].state,
        ControlActionState::ImportedUnreconciled as i32
    );
    assert_eq!(snapshot.completed_actions, [3, 9]);
    assert_eq!(snapshot.control_revision, next);
    assert_eq!(snapshot.digest.len(), 32);

    // The staged references are gone; the retained chunk commands still
    // answer an exact retry with their stored decision.
    let workflow = store
        .control_import_workflow("alice", &key("books"), b"import-1")
        .unwrap();
    assert_eq!(workflow.phase, ControlImportPhase::Committed as i32);
    assert_eq!(workflow.reserved_bytes, 0);
    assert_eq!(workflow.reserved_decisions, 0);
    let retry = store
        .execute_control_import(
            "alice",
            &command(
                &authority,
                "chunk-0",
                2,
                chunk_action(&payload, workflow.declared.as_ref().unwrap().chunk_bytes, 0),
            ),
        )
        .unwrap();
    assert_eq!(retry.code, 0);
    assert_eq!(retry.control_revision, 3);

    // Reopen recomputes every count and byte; the committed view is identical.
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(
        reopened.control_snapshot("alice", &key("books")).unwrap(),
        snapshot
    );
    // Nothing else may import into this resource again.
    let (chunk_bytes, chunk_count) =
        plan_chunks(&limits, &authority, &key("books"), &payload).unwrap();
    let again = reopened
        .begin_control_import(
            "alice",
            &{
                let mut c = command(
                    &authority,
                    "begin-2",
                    next,
                    begin_action(&retired, &payload, chunk_bytes, chunk_count),
                );
                c.workflow_id = b"import-2".to_vec();
                c
            },
            &retired,
        )
        .unwrap();
    assert_eq!(again.code, Code::AlreadyExists as u32, "{}", again.message);
}

#[test]
fn begin_binds_the_retirement_holder_to_authority_actor_resource_and_digests() {
    let dir = Directory::new("admission");
    let limits = limits(64 << 10, 64);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    let legacy = legacy(&dir, 2);
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let payload = payload(&retired, supplement(&checkpoint));
    let begin = begin_action(&retired, &payload, payload.len() as u32, 1);

    // The general command path never admits a Begin: bytes are not the proof.
    assert_eq!(
        store
            .execute_control_import("alice", &command(&authority, "b", 1, begin.clone()))
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    // Another administrator cannot import alice's retirement.
    assert_eq!(
        store
            .begin_control_import("bob", &command(&authority, "b", 1, begin.clone()), &retired)
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    // Another destination authority cannot consume it either.
    let other_dir = Directory::new("admission-other");
    let other =
        SourceAuthorityStore::create(&other_dir.authority(), &identity(9), &policy(), &limits)
            .unwrap();
    let mut foreign = command(&identity(9), "b", 1, begin.clone());
    foreign.authority = Some(identity(9));
    assert_eq!(
        other
            .begin_control_import("alice", &foreign, &retired)
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    // Digests in the command must match the holder.
    let mut forged = match begin.clone() {
        ImportAction::Begin(b) => b,
        _ => unreachable!(),
    };
    forged.retirement_sha256 = vec![0xab; 32];
    assert_eq!(
        store
            .begin_control_import(
                "alice",
                &command(&authority, "b", 1, ImportAction::Begin(forged)),
                &retired
            )
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    // Proposal admission records nothing: the id stays free.
    assert_eq!(
        store
            .control_import_decision("alice", &key("books"), b"b")
            .err()
            .unwrap()
            .code(),
        Code::NotFound
    );
    // A payload whose retirement bytes are a forgery — syntactically valid,
    // bound to the wrong checkpoint — refuses at Commit, durably.
    let mut record = retired.record().clone();
    record.request.as_mut().unwrap().command_id = b"forged".to_vec();
    let forged_payload = ControlImportPayload {
        format_version: 1,
        retirement: record.encode_to_vec(),
        supplement: Some(supplement(&checkpoint)),
    }
    .encode_to_vec();
    let accepted = store
        .begin_control_import(
            "alice",
            &command(
                &authority,
                "begin",
                1,
                begin_action(&retired, &forged_payload, forged_payload.len() as u32, 1),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(accepted.code, 0, "{}", accepted.message);
    assert_eq!(
        store
            .execute_control_import(
                "alice",
                &command(
                    &authority,
                    "c0",
                    2,
                    chunk_action(&forged_payload, forged_payload.len() as u32, 0)
                )
            )
            .unwrap()
            .code,
        0
    );
    let commit = store
        .execute_control_import(
            "alice",
            &command(
                &authority,
                "commit",
                3,
                ImportAction::Commit(CommitControlImport {}),
            ),
        )
        .unwrap();
    assert_eq!(commit.code, Code::FailedPrecondition as u32);
    assert!(
        commit.message.contains("retirement bytes differ"),
        "{}",
        commit.message
    );
    assert_eq!(
        store
            .control_snapshot("alice", &key("books"))
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    // The workflow stays pending and can be abandoned; the retained history
    // still keeps the rejected commit.
    let abort = store
        .execute_control_import(
            "alice",
            &command(
                &authority,
                "abort",
                3,
                ImportAction::Abort(AbortControlImport {}),
            ),
        )
        .unwrap();
    assert_eq!(abort.code, 0, "{}", abort.message);
    assert_eq!(
        store
            .control_import_decision("alice", &key("books"), b"commit")
            .unwrap()
            .code,
        Code::FailedPrecondition as u32
    );
    drop(store);
    SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
}

#[test]
fn chunk_capacity_follows_the_configured_command_limit() {
    let authority = identity(7);
    let tight = limits(MIN_CHUNK_CAPACITY as u32, 64);
    assert_eq!(
        chunk_capacity(&tight, &authority, &key("books"))
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let dir = Directory::new("capacity");
    let limits = limits(12 << 10, 64);
    let store = store(&dir, 7, &limits);
    let legacy = legacy(&dir, 2);
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let payload = payload(&retired, supplement(&checkpoint));
    let capacity = chunk_capacity(&limits, &authority, &key("books")).unwrap();
    assert!(payload.len() < capacity);

    // A plan declaring chunks larger than the capacity is a recorded refusal.
    let oversized = store
        .begin_control_import(
            "alice",
            &command(
                &authority,
                "begin-big",
                1,
                begin_action(&retired, &payload, capacity as u32 + 1, 1),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(
        oversized.code,
        Code::InvalidArgument as u32,
        "{}",
        oversized.message
    );

    // Chunks exactly at capacity fit the command bound; one byte more does not.
    let mut wide = payload.clone();
    wide.resize(capacity * 2, 0x5a);
    let mut c = command(
        &authority,
        "probe",
        u64::MAX,
        chunk_action(&wide, capacity as u32, 0),
    );
    c.expected_policy_revision = u64::MAX;
    c.command_id = vec![0xff; 1024];
    c.workflow_id = vec![0xff; 1024];
    if let Some(ImportAction::Chunk(chunk)) = c.action.as_mut() {
        chunk.ordinal = u32::MAX;
    }
    assert!(c.encoded_len() <= limits.max_command_bytes as usize);
    if let Some(ImportAction::Chunk(chunk)) = c.action.as_mut() {
        chunk.bytes.push(0);
        chunk.sha256 = chunk_digest(&chunk.bytes);
    }
    assert!(c.encoded_len() > limits.max_command_bytes as usize);
    assert_eq!(
        store
            .execute_control_import("alice", &c)
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
}

#[test]
fn retries_actors_revisions_and_terminal_states_are_pinned() {
    let dir = Directory::new("rules");
    let limits = limits(8 << 10, 128);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    let legacy = legacy(&dir, 120);
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let payload = payload(&retired, supplement(&checkpoint));
    let (chunk_bytes, chunk_count) =
        plan_chunks(&limits, &authority, &key("books"), &payload).unwrap();
    assert!(chunk_count >= 2);
    let begin = store
        .begin_control_import(
            "alice",
            &command(
                &authority,
                "begin",
                1,
                begin_action(&retired, &payload, chunk_bytes, chunk_count),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(begin.code, 0);
    let first = store
        .execute_control_import(
            "alice",
            &command(
                &authority,
                "chunk-0",
                2,
                chunk_action(&payload, chunk_bytes, 0),
            ),
        )
        .unwrap();
    assert_eq!(first.code, 0);

    // Exact retry: the stored decision, no new revision.
    let retry = store
        .execute_control_import(
            "alice",
            &command(
                &authority,
                "chunk-0",
                2,
                chunk_action(&payload, chunk_bytes, 0),
            ),
        )
        .unwrap();
    assert_eq!(retry, first);
    // A new id with the same content is a recorded AlreadyExists; different
    // content under an already staged ordinal is a recorded FailedPrecondition.
    let same = store
        .execute_control_import(
            "alice",
            &command(
                &authority,
                "chunk-0-again",
                3,
                chunk_action(&payload, chunk_bytes, 0),
            ),
        )
        .unwrap();
    assert_eq!(same.code, Code::AlreadyExists as u32, "{}", same.message);
    assert_eq!(same.control_revision, 3);
    let mut different = chunk_action(&payload, chunk_bytes, 0);
    if let ImportAction::Chunk(chunk) = &mut different {
        chunk.bytes[0] ^= 1;
        chunk.sha256 = chunk_digest(&chunk.bytes);
    }
    let conflict = store
        .execute_control_import("alice", &command(&authority, "chunk-0-other", 3, different))
        .unwrap();
    assert_eq!(conflict.code, Code::FailedPrecondition as u32);
    // Another administrator cannot continue or abort alice's workflow.
    let intruder = store
        .execute_control_import(
            "bob",
            &command(
                &authority,
                "bob-abort",
                3,
                ImportAction::Abort(AbortControlImport {}),
            ),
        )
        .unwrap();
    assert_eq!(
        intruder.code,
        Code::FailedPrecondition as u32,
        "{}",
        intruder.message
    );
    // Same command_id namespace as ordinary commands.
    assert_eq!(
        store
            .execute(
                "alice",
                &SourceAuthorityCommand {
                    format_version: 1,
                    authority: Some(authority.clone()),
                    key: Some(key("books")),
                    command_id: b"chunk-0".to_vec(),
                    expected_control_revision: 3,
                    expected_policy_revision: 1,
                    expected_ownership_generation: 0,
                    action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
                        grants: vec![grant("alice", "books"), grant("bob", "books")],
                    })),
                },
            )
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    // An unrelated committed command moves the revision; a stale chunk is a
    // recorded CAS refusal and the same content lands with the current one.
    let unrelated = store
        .execute(
            "bob",
            &SourceAuthorityCommand {
                format_version: 1,
                authority: Some(authority.clone()),
                key: Some(key("books")),
                command_id: b"bob-grants".to_vec(),
                expected_control_revision: 3,
                expected_policy_revision: 1,
                expected_ownership_generation: 0,
                action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
                    grants: vec![grant("alice", "books"), grant("bob", "books")],
                })),
            },
        )
        .unwrap();
    assert_eq!(unrelated.code, 0, "{}", unrelated.message);
    assert_eq!(unrelated.control_revision, 4);
    assert_eq!(unrelated.policy_revision, 2);
    let mut stale = command(
        &authority,
        "chunk-1-stale",
        3,
        chunk_action(&payload, chunk_bytes, 1),
    );
    stale.expected_policy_revision = 2;
    let stale = store.execute_control_import("alice", &stale).unwrap();
    assert_eq!(stale.code, Code::FailedPrecondition as u32);
    let mut revision = 4;
    for ordinal in 1..chunk_count {
        let mut c = command(
            &authority,
            &format!("chunk-{ordinal}"),
            revision,
            chunk_action(&payload, chunk_bytes, ordinal),
        );
        c.expected_policy_revision = 2;
        let decision = store.execute_control_import("alice", &c).unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
        revision += 1;
    }
    // Revocation between steps is enforced at the next step.
    let revoke = store
        .execute(
            "bob",
            &SourceAuthorityCommand {
                format_version: 1,
                authority: Some(authority.clone()),
                key: Some(key("books")),
                command_id: b"bob-revokes".to_vec(),
                expected_control_revision: revision,
                expected_policy_revision: 2,
                expected_ownership_generation: 0,
                action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
                    grants: vec![grant("bob", "books")],
                })),
            },
        )
        .unwrap();
    assert_eq!(revoke.code, 0);
    revision += 1;
    let mut commit = command(
        &authority,
        "commit",
        revision,
        ImportAction::Commit(CommitControlImport {}),
    );
    commit.expected_policy_revision = 3;
    assert_eq!(
        store
            .execute_control_import("alice", &commit)
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    // Restored rights let the initiating actor finish; nobody else can.
    let restore = store
        .execute(
            "bob",
            &SourceAuthorityCommand {
                format_version: 1,
                authority: Some(authority.clone()),
                key: Some(key("books")),
                command_id: b"bob-restores".to_vec(),
                expected_control_revision: revision,
                expected_policy_revision: 3,
                expected_ownership_generation: 0,
                action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
                    grants: vec![grant("alice", "books"), grant("bob", "books")],
                })),
            },
        )
        .unwrap();
    assert_eq!(restore.code, 0);
    revision += 1;
    let mut bob_commit = command(
        &authority,
        "bob-commit",
        revision,
        ImportAction::Commit(CommitControlImport {}),
    );
    bob_commit.expected_policy_revision = 4;
    let bob_commit = store.execute_control_import("bob", &bob_commit).unwrap();
    assert_eq!(bob_commit.code, Code::FailedPrecondition as u32);
    let mut commit = command(
        &authority,
        "commit-2",
        revision,
        ImportAction::Commit(CommitControlImport {}),
    );
    commit.expected_policy_revision = 4;
    let commit = store.execute_control_import("alice", &commit).unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
    revision += 1;
    // Terminal: a late chunk, a second commit and an abort all refuse durably.
    for (id, action) in [
        ("late-chunk", chunk_action(&payload, chunk_bytes, 0)),
        ("commit-3", ImportAction::Commit(CommitControlImport {})),
        ("abort-late", ImportAction::Abort(AbortControlImport {})),
    ] {
        let mut c = command(&authority, id, revision, action);
        c.expected_policy_revision = 4;
        let decision = store.execute_control_import("alice", &c).unwrap();
        assert_eq!(
            decision.code,
            Code::FailedPrecondition as u32,
            "{id}: {}",
            decision.message
        );
    }
    drop(store);
    SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
}

#[test]
fn terminal_headroom_is_reserved_while_an_import_is_pending() {
    let dir = Directory::new("headroom");
    let authority = identity(7);
    let legacy = legacy(&dir, 2);
    // Enough decisions for begin + 1 chunk + 2 terminal + retirement-free
    // bookkeeping, plus exactly one unrelated command.
    let limits = limits(64 << 10, 5);
    let store = store(&dir, 7, &limits);
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let payload = payload(&retired, supplement(&checkpoint));
    let begin = store
        .begin_control_import(
            "alice",
            &command(
                &authority,
                "begin",
                1,
                begin_action(&retired, &payload, payload.len() as u32, 1),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(begin.code, 0, "{}", begin.message);
    let grants = |id: &str, revision: u64| SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("music")),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: revision,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants: vec![grant("alice", "music")],
        })),
    };
    // 1 used + 3 reserved < 5: one unrelated command fits; the next does not.
    assert_eq!(store.execute("alice", &grants("g1", 2)).unwrap().code, 0);
    assert_eq!(
        store.execute("alice", &grants("g2", 3)).unwrap_err().code(),
        Code::ResourceExhausted
    );
    // The pending import still completes out of its own reservation.
    let mut chunk = command(
        &authority,
        "c0",
        3,
        chunk_action(&payload, payload.len() as u32, 0),
    );
    chunk.expected_policy_revision = 2;
    assert_eq!(
        store.execute_control_import("alice", &chunk).unwrap().code,
        0
    );
    let mut commit = command(
        &authority,
        "commit",
        4,
        ImportAction::Commit(CommitControlImport {}),
    );
    commit.expected_policy_revision = 2;
    let commit = store.execute_control_import("alice", &commit).unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
    let tx = store.inner.database().begin_read().unwrap();
    let meta = tx.open_table(META).unwrap();
    let control: ControlStoreHeader =
        contract::decode(meta.get(CONTROL_HEADER).unwrap().unwrap().value()).unwrap();
    assert_eq!((control.reserved_bytes, control.reserved_decisions), (0, 0));
    assert_eq!(control.staged_chunk_count, 0);
    drop(tx);
    drop(store);
    SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
}

#[test]
fn supplement_refusals_are_durable_and_leave_no_applied_state() {
    let dir = Directory::new("supplement");
    let limits = limits(64 << 10, 128);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    let legacy = legacy(&dir, 2);
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let mut revision = 1;
    let tree = PlacementTree {
        column: "placement".into(),
        level_bits: 0,
        nodes: vec![
            PlacementNode {
                name: "old".into(),
                cel: "year < 2020".into(),
                ..Default::default()
            },
            PlacementNode {
                name: "rest".into(),
                ..Default::default()
            },
        ],
    };
    let placement = crate::placement::Placement::validate(
        &crate::placement::PlacementTreeConfig::from_proto(&tree),
    )
    .unwrap();
    let leaf = |name: &str| placement.leaf_by_name(name).unwrap().code as u64;
    let mut wrong_policy = supplement(&checkpoint);
    wrong_policy
        .policy
        .as_mut()
        .unwrap()
        .control
        .as_mut()
        .unwrap()
        .lease_ms += 1;
    let mut missing_codes = supplement(&checkpoint);
    missing_codes.placement = Some(Placement::Tree(tree.clone()));
    let mut bad_code = missing_codes.clone();
    bad_code.route_codes = vec![
        PlacementRouteCode {
            has_placement: true,
            placement: leaf("old"),
        },
        PlacementRouteCode {
            has_placement: true,
            placement: 1 << 62,
        },
    ];
    let mut no_history_flag = supplement(&checkpoint);
    no_history_flag.history_placement_unavailable = false;
    let mut bad_fingerprint = supplement(&checkpoint);
    bad_fingerprint.derived = Some(Derived::Declaration(DerivedColumns {
        columns: vec![DerivedColumn {
            name: "year_next".into(),
            expression: "year + 1".into(),
            kind: MaterializeKind::I64 as i32,
            disclosure: DerivedDisclosure::Inputs as i32,
        }],
    }));
    bad_fingerprint.derived_fingerprint = "wrong".into();
    let mut absent_provider = supplement(&checkpoint);
    absent_provider.provider = None;
    for (name, supplement) in [
        ("policy", wrong_policy),
        ("codes", missing_codes),
        ("leaf", bad_code),
        ("history", no_history_flag),
        ("fingerprint", bad_fingerprint),
        ("provider", absent_provider),
    ] {
        let payload = payload(&retired, supplement);
        let mut begin = command(
            &authority,
            &format!("begin-{name}"),
            revision,
            begin_action(&retired, &payload, payload.len() as u32, 1),
        );
        begin.workflow_id = name.as_bytes().to_vec();
        assert_eq!(
            store
                .begin_control_import("alice", &begin, &retired)
                .unwrap()
                .code,
            0
        );
        revision += 1;
        let mut chunk = command(
            &authority,
            &format!("chunk-{name}"),
            revision,
            chunk_action(&payload, payload.len() as u32, 0),
        );
        chunk.workflow_id = name.as_bytes().to_vec();
        assert_eq!(
            store.execute_control_import("alice", &chunk).unwrap().code,
            0
        );
        revision += 1;
        let mut commit = command(
            &authority,
            &format!("commit-{name}"),
            revision,
            ImportAction::Commit(CommitControlImport {}),
        );
        commit.workflow_id = name.as_bytes().to_vec();
        let commit = store.execute_control_import("alice", &commit).unwrap();
        assert_eq!(
            commit.code,
            Code::FailedPrecondition as u32,
            "{name}: {}",
            commit.message
        );
        assert!(
            commit.message.contains("supplement"),
            "{name}: {}",
            commit.message
        );
        assert_eq!(
            store
                .control_snapshot("alice", &key("books"))
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        let mut abort = command(
            &authority,
            &format!("abort-{name}"),
            revision,
            ImportAction::Abort(AbortControlImport {}),
        );
        abort.workflow_id = name.as_bytes().to_vec();
        assert_eq!(
            store.execute_control_import("alice", &abort).unwrap().code,
            0
        );
        revision += 1;
    }
    // A complete tree with one code per route applies and is served back.
    let mut with_tree = supplement(&checkpoint);
    with_tree.placement = Some(Placement::Tree(tree.clone()));
    with_tree.route_codes = vec![
        PlacementRouteCode {
            has_placement: true,
            placement: leaf("old"),
        },
        PlacementRouteCode {
            has_placement: true,
            placement: leaf("rest"),
        },
    ];
    let payload = payload(&retired, with_tree.clone());
    let mut begin = command(
        &authority,
        "begin-tree",
        revision,
        begin_action(&retired, &payload, payload.len() as u32, 1),
    );
    begin.workflow_id = b"tree".to_vec();
    assert_eq!(
        store
            .begin_control_import("alice", &begin, &retired)
            .unwrap()
            .code,
        0
    );
    revision += 1;
    let mut chunk = command(
        &authority,
        "chunk-tree",
        revision,
        chunk_action(&payload, payload.len() as u32, 0),
    );
    chunk.workflow_id = b"tree".to_vec();
    assert_eq!(
        store.execute_control_import("alice", &chunk).unwrap().code,
        0
    );
    revision += 1;
    let mut commit = command(
        &authority,
        "commit-tree",
        revision,
        ImportAction::Commit(CommitControlImport {}),
    );
    commit.workflow_id = b"tree".to_vec();
    let commit = store.execute_control_import("alice", &commit).unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
    let snapshot = store.control_snapshot("alice", &key("books")).unwrap();
    let topology = snapshot.topology.unwrap();
    assert!(topology.codes_available);
    assert_eq!(topology.codes, with_tree.route_codes);
    assert_eq!(
        snapshot.state.unwrap().configuration.unwrap().placement,
        Some(Placement::Tree(tree))
    );
    drop(store);
    SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
}

#[test]
fn replaying_the_retained_commands_reproduces_the_identical_store() {
    let dir = Directory::new("replay");
    let limits = limits(8 << 10, 128);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    let legacy = legacy(&dir, 150);
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let payload = payload(&retired, supplement(&checkpoint));
    run_import(&store, &authority, &limits, &retired, &payload, 1);
    let recorded: Vec<ControlImportCommand> = {
        let tx = store.inner.database().begin_read().unwrap();
        let ops = tx.open_table(IMPORT_OPERATIONS).unwrap();
        let mut commands: Vec<ControlImportOperation> = ops
            .iter()
            .unwrap()
            .map(|e| contract::decode(e.unwrap().1.value()).unwrap())
            .collect();
        commands.sort_by_key(|op| op.decision.as_ref().unwrap().control_revision);
        commands.into_iter().map(|op| op.command.unwrap()).collect()
    };
    let original = table_bytes(&store);
    let original_snapshot = store.control_snapshot("alice", &key("books")).unwrap();

    // The same admitted retirement fact and the same command sequence applied
    // to a fresh store of the same identity produce identical bytes.
    let replica_dir = Directory::new("replay-replica");
    let replica =
        SourceAuthorityStore::create(&replica_dir.authority(), &authority, &policy(), &limits)
            .unwrap();
    for command in &recorded {
        let decision = if matches!(command.action, Some(ImportAction::Begin(_))) {
            replica.replay_control_import("alice", command).unwrap()
        } else {
            replica.execute_control_import("alice", command).unwrap()
        };
        assert_eq!(decision.code, 0, "{}", decision.message);
    }
    assert_eq!(table_bytes(&replica), original);
    assert_eq!(
        replica.control_snapshot("alice", &key("books")).unwrap(),
        original_snapshot
    );
}

#[test]
fn import_exit_worker() {
    let Some(mode) = std::env::var_os("PSEARCH_CONTROL_IMPORT_EXIT_FAULT") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("PSEARCH_CONTROL_IMPORT_DIR").unwrap());
    let mode = mode.to_str().unwrap().to_string();
    let (phase, fault) = mode.split_once(':').unwrap();
    let limits = limits(64 << 10, 64);
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&root.join("authority.redb"), &authority, &policy(), &limits)
            .unwrap();
    let legacy = DurableControlPlane::open_existing(
        root.join("legacy.json"),
        test_fixtures::populated_policy(),
    )
    .unwrap()
    .with_collection("books")
    .unwrap();
    let (retired, request) = retire(&store, &legacy, 1);
    std::fs::write(root.join("request.bin"), request.encode_to_vec()).unwrap();
    let checkpoint = checkpoint_of(&retired);
    let payload = payload(&retired, supplement(&checkpoint));
    let begin = command(
        &authority,
        "begin",
        1,
        begin_action(&retired, &payload, payload.len() as u32, 1),
    );
    let chunk = command(
        &authority,
        "c0",
        2,
        chunk_action(&payload, payload.len() as u32, 0),
    );
    let commit = command(
        &authority,
        "commit",
        3,
        ImportAction::Commit(CommitControlImport {}),
    );
    let arm = |store: &SourceAuthorityStore| {
        *store.inner.fault.lock().unwrap() = Some(match fault {
            "before" => Fault::ExitBeforeCommit,
            "after" => Fault::ExitAfterCommit,
            other => panic!("unknown exit fault {other}"),
        });
    };
    if phase == "begin" {
        arm(&store);
    }
    store
        .begin_control_import("alice", &begin, &retired)
        .unwrap();
    if phase == "chunk" {
        arm(&store);
    }
    store.execute_control_import("alice", &chunk).unwrap();
    if phase == "commit" {
        arm(&store);
    }
    store.execute_control_import("alice", &commit).unwrap();
    panic!("exit fault did not terminate the worker");
}

#[test]
fn abrupt_exit_at_every_phase_recovers_identical_accounting_and_finishes() {
    for phase in ["begin", "chunk", "commit"] {
        for fault in ["before", "after"] {
            let dir = Directory::new(&format!("exit-{phase}-{fault}"));
            std::fs::write(
                dir.legacy(),
                test_fixtures::populated_state_json("books", 2),
            )
            .unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("source_authority::import_tests::import_exit_worker")
                .arg("--nocapture")
                .env(
                    "PSEARCH_CONTROL_IMPORT_EXIT_FAULT",
                    format!("{phase}:{fault}"),
                )
                .env("PSEARCH_CONTROL_IMPORT_DIR", &dir.0)
                .status_guarded()
                .unwrap();
            assert_eq!(status.code(), Some(87), "{phase}:{fault}");

            let authority = identity(7);
            let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
            // The retirement is durable in the legacy file regardless of where
            // the import stopped; recover it to continue.
            let request = LegacyControlRetirementRequest::decode(
                std::fs::read(dir.0.join("request.bin")).unwrap().as_slice(),
            )
            .unwrap();
            let retired = reopened
                .recover_legacy_retirement("alice", &request, &dir.legacy())
                .unwrap();
            let checkpoint = checkpoint_of(&retired);
            let payload = payload(&retired, supplement(&checkpoint));
            let steps = [("begin", 1u64), ("chunk", 2), ("commit", 3)];
            let completed_steps = steps.iter().position(|(p, _)| *p == phase).unwrap()
                + usize::from(fault == "after");
            for (index, (step, revision)) in steps.iter().enumerate() {
                let command = match *step {
                    "begin" => command(
                        &authority,
                        "begin",
                        *revision,
                        begin_action(&retired, &payload, payload.len() as u32, 1),
                    ),
                    "chunk" => command(
                        &authority,
                        "c0",
                        *revision,
                        chunk_action(&payload, payload.len() as u32, 0),
                    ),
                    _ => command(
                        &authority,
                        "commit",
                        *revision,
                        ImportAction::Commit(CommitControlImport {}),
                    ),
                };
                let stored =
                    reopened.control_import_decision("alice", &key("books"), &command.command_id);
                if index < completed_steps {
                    assert_eq!(
                        stored.unwrap().code,
                        0,
                        "{phase}:{fault} step {step} should be durable"
                    );
                } else {
                    assert_eq!(
                        stored.unwrap_err().code(),
                        Code::NotFound,
                        "{phase}:{fault} step {step} should be absent"
                    );
                }
                let decision = if *step == "begin" {
                    reopened
                        .begin_control_import("alice", &command, &retired)
                        .unwrap()
                } else {
                    reopened.execute_control_import("alice", &command).unwrap()
                };
                assert_eq!(
                    decision.code, 0,
                    "{phase}:{fault} {step}: {}",
                    decision.message
                );
                assert_eq!(decision.control_revision, revision + 1);
            }
            let snapshot = reopened.control_snapshot("alice", &key("books")).unwrap();
            assert_eq!(snapshot.control_revision, 4);
            drop(reopened);
            let again = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
            assert_eq!(
                again.control_snapshot("alice", &key("books")).unwrap(),
                snapshot
            );
        }
    }
}

#[test]
fn format_one_stores_adopt_and_incomplete_stores_refuse() {
    let dir = Directory::new("adopt");
    let authority = identity(7);
    let limits = limits(64 << 10, 64);
    let store = store(&dir, 7, &limits);
    // Strip the format-2 tables and header: the shape a format-1 store has.
    {
        let tx = store.inner.database().begin_write().unwrap();
        for definition in [
            IMPORT_OPERATIONS,
            IMPORTS,
            CHUNKS,
            CONTROL,
            TOPOLOGIES,
            NODES,
            REPLICAS,
            ACTIONS,
            COMPLETED,
            capacity::CAPACITY_OPERATIONS,
            capacity::CAPACITY,
            capacity::REPORTERS,
            capacity::OBSERVATIONS,
        ] {
            tx.delete_table(definition).unwrap();
        }
        {
            let mut meta = tx.open_table(META).unwrap();
            meta.remove(CONTROL_HEADER).unwrap();
            meta.remove(capacity::CAPACITY_HEADER).unwrap();
        }
        tx.commit().unwrap();
    }
    drop(store);
    let adopted = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(
        adopted
            .control_snapshot("alice", &key("books"))
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    // A store missing one control table is neither format and refuses.
    {
        let tx = adopted.inner.database().begin_write().unwrap();
        tx.delete_table(COMPLETED).unwrap();
        tx.commit().unwrap();
    }
    drop(adopted);
    assert_eq!(
        SourceAuthorityStore::open(&dir.authority(), &authority)
            .err()
            .unwrap()
            .code(),
        Code::DataLoss
    );
    // Restore the table, then a future control format refuses by name.
    let dir2 = Directory::new("future");
    let store2 =
        SourceAuthorityStore::create(&dir2.authority(), &authority, &policy(), &limits).unwrap();
    {
        let tx = store2.inner.database().begin_write().unwrap();
        {
            let mut meta = tx.open_table(META).unwrap();
            meta.insert(
                CONTROL_HEADER,
                ControlStoreHeader {
                    format_version: 3,
                    ..Default::default()
                }
                .encode_to_vec()
                .as_slice(),
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    drop(store2);
    let error = SourceAuthorityStore::open(&dir2.authority(), &authority)
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::DataLoss);
    assert!(error.message().contains("format 2"), "{error}");
}

fn grants(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control: u64,
    policy_revision: u64,
    grants: Vec<CollectionGrant>,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key("books")),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control,
        expected_policy_revision: policy_revision,
        expected_ownership_generation: 0,
        action: Some(Action::ReplaceGrants(ReplaceSourceCollectionGrants {
            grants,
        })),
    }
}

fn recover(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control: u64,
    policy_revision: u64,
) -> ControlImportCommand {
    let mut c = command(
        authority,
        id,
        control,
        ImportAction::Recover(RecoverControlImport {}),
    );
    c.expected_policy_revision = policy_revision;
    c
}

fn retained_operations(store: &SourceAuthorityStore) -> u64 {
    let tx = store.inner.database().begin_read().unwrap();
    tx.open_table(IMPORT_OPERATIONS).unwrap().len().unwrap()
}

#[test]
fn administrative_recovery_terminates_a_stranded_import_without_transfer() {
    let dir = Directory::new("admin-recovery");
    let limits = limits(8 << 10, 128);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    let legacy = legacy(&dir, 120);
    let (retired, request) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let payload = payload(&retired, supplement(&checkpoint));
    let (chunk_bytes, chunk_count) =
        plan_chunks(&limits, &authority, &key("books"), &payload).unwrap();
    assert!(chunk_count >= 2);
    assert_eq!(
        store
            .begin_control_import(
                "alice",
                &command(
                    &authority,
                    "begin",
                    1,
                    begin_action(&retired, &payload, chunk_bytes, chunk_count)
                ),
                &retired
            )
            .unwrap()
            .code,
        0
    );
    assert_eq!(
        store
            .execute_control_import(
                "alice",
                &command(
                    &authority,
                    "chunk-0",
                    2,
                    chunk_action(&payload, chunk_bytes, 0)
                )
            )
            .unwrap()
            .code,
        0
    );
    let retained = retained_operations(&store);
    // Nobody but the initiator can continue or abort; a recovery by another
    // administrator before any revocation is also an abort-only step.
    let intruder = store
        .execute_control_import(
            "bob",
            &command(
                &authority,
                "bob-abort",
                3,
                ImportAction::Abort(AbortControlImport {}),
            ),
        )
        .unwrap();
    assert_eq!(
        intruder.code,
        Code::FailedPrecondition as u32,
        "{}",
        intruder.message
    );
    // The initiator loses Admin: her steps refuse and the reservation stays.
    assert_eq!(
        store
            .execute(
                "bob",
                &grants(&authority, "bob-revokes", 3, 1, vec![grant("bob", "books")])
            )
            .unwrap()
            .code,
        0
    );
    let mut stranded = command(
        &authority,
        "chunk-1",
        5,
        chunk_action(&payload, chunk_bytes, 1),
    );
    stranded.expected_policy_revision = 2;
    assert_eq!(
        store
            .execute_control_import("alice", &stranded)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    let workflow = store
        .control_import_workflow("bob", &key("books"), b"import-1")
        .unwrap();
    assert_eq!(workflow.phase, ControlImportPhase::Staging as i32);
    assert!(workflow.reserved_bytes > 0 && workflow.reserved_decisions > 0);
    // Stale revision refuses durably; the current one recovers.
    let stale = store
        .execute_control_import("bob", &recover(&authority, "bob-recover-stale", 3, 2))
        .unwrap();
    assert_eq!(stale.code, Code::FailedPrecondition as u32);
    let recovered = store
        .execute_control_import("bob", &recover(&authority, "bob-recover", 4, 2))
        .unwrap();
    assert_eq!(recovered.code, 0, "{}", recovered.message);
    assert_eq!(recovered.control_revision, 5);
    assert_eq!(
        store
            .execute_control_import("bob", &recover(&authority, "bob-recover", 4, 2))
            .unwrap(),
        recovered
    );
    let workflow = store
        .control_import_workflow("bob", &key("books"), b"import-1")
        .unwrap();
    assert_eq!(workflow.phase, ControlImportPhase::Recovered as i32);
    assert_eq!(
        workflow.principal, "alice",
        "the workflow keeps its initiator"
    );
    assert_eq!(
        workflow.terminal.as_ref().unwrap().principal,
        "bob",
        "the terminal step names the recoverer"
    );
    assert_eq!(
        (workflow.reserved_bytes, workflow.reserved_decisions),
        (0, 0)
    );
    assert_eq!(workflow.staged_chunks, 1);
    // Charged history is retained: the staged chunk's command and the two
    // recovery decisions are there; the references are gone; the store has
    // no applied control state and the resource remains importable.
    assert_eq!(retained_operations(&store), retained + 3);
    {
        let tx = store.inner.database().begin_read().unwrap();
        assert_eq!(tx.open_table(CHUNKS).unwrap().len().unwrap(), 0);
        let meta = tx.open_table(META).unwrap();
        let control: ControlStoreHeader =
            contract::decode(meta.get(CONTROL_HEADER).unwrap().unwrap().value()).unwrap();
        assert_eq!(
            (
                control.reserved_bytes,
                control.reserved_decisions,
                control.staged_chunk_count
            ),
            (0, 0, 0)
        );
    }
    assert_eq!(
        store
            .control_snapshot("bob", &key("books"))
            .err()
            .unwrap()
            .code(),
        Code::NotFound
    );
    // Terminal: a second recovery and a later commit or abort all refuse.
    for (actor, id, action) in [
        (
            "bob",
            "bob-recover-2",
            ImportAction::Recover(RecoverControlImport {}),
        ),
        (
            "bob",
            "bob-commit",
            ImportAction::Commit(CommitControlImport {}),
        ),
    ] {
        let mut c = command(&authority, id, 5, action);
        c.expected_policy_revision = 2;
        let decision = store.execute_control_import(actor, &c).unwrap();
        assert_eq!(
            decision.code,
            Code::FailedPrecondition as u32,
            "{id}: {}",
            decision.message
        );
    }
    // Recovery did not transfer the retirement: bob cannot begin with
    // alice's holder, and the legacy file stays retired.
    let mut bob_begin = command(
        &authority,
        "bob-begin",
        5,
        begin_action(&retired, &payload, chunk_bytes, chunk_count),
    );
    bob_begin.expected_policy_revision = 2;
    bob_begin.workflow_id = b"import-2".to_vec();
    assert_eq!(
        store
            .begin_control_import("bob", &bob_begin, &retired)
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        legacy.checkpoint_for_import().err().unwrap().code(),
        Code::FailedPrecondition
    );
    // With rights restored, the initiator recovers her retirement holder
    // and imports under a new workflow id; the old one is never reusable.
    assert_eq!(
        store
            .execute(
                "bob",
                &grants(
                    &authority,
                    "bob-restores",
                    5,
                    2,
                    vec![grant("alice", "books"), grant("bob", "books")]
                )
            )
            .unwrap()
            .code,
        0
    );
    drop(retired);
    drop(legacy);
    let holder = store
        .recover_legacy_retirement("alice", &request, &dir.legacy())
        .unwrap();
    let mut reused = command(
        &authority,
        "begin-reuse",
        6,
        begin_action(&holder, &payload, chunk_bytes, chunk_count),
    );
    reused.expected_policy_revision = 3;
    let reused = store
        .begin_control_import("alice", &reused, &holder)
        .unwrap();
    assert_eq!(
        reused.code,
        Code::AlreadyExists as u32,
        "{}",
        reused.message
    );
    let mut fresh = command(
        &authority,
        "begin-2",
        6,
        begin_action(&holder, &payload, chunk_bytes, chunk_count),
    );
    fresh.expected_policy_revision = 3;
    fresh.workflow_id = b"import-2".to_vec();
    let fresh = store
        .begin_control_import("alice", &fresh, &holder)
        .unwrap();
    assert_eq!(fresh.code, 0, "{}", fresh.message);
    let mut revision = 7;
    for ordinal in 0..chunk_count {
        let mut c = command(
            &authority,
            &format!("c2-{ordinal}"),
            revision,
            chunk_action(&payload, chunk_bytes, ordinal),
        );
        c.expected_policy_revision = 3;
        c.workflow_id = b"import-2".to_vec();
        assert_eq!(store.execute_control_import("alice", &c).unwrap().code, 0);
        revision += 1;
    }
    let mut commit = command(
        &authority,
        "commit-2",
        revision,
        ImportAction::Commit(CommitControlImport {}),
    );
    commit.expected_policy_revision = 3;
    commit.workflow_id = b"import-2".to_vec();
    let commit = store.execute_control_import("alice", &commit).unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
    assert_eq!(commit.receipt.unwrap().routes, 120);
    drop(store);
    SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
}

#[test]
fn a_commit_and_a_recovery_race_to_exactly_one_terminal_decision() {
    let dir = Directory::new("race");
    let limits = limits(64 << 10, 128);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    let legacy = legacy(&dir, 2);
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let payload = payload(&retired, supplement(&checkpoint));
    assert_eq!(
        store
            .begin_control_import(
                "alice",
                &command(
                    &authority,
                    "begin",
                    1,
                    begin_action(&retired, &payload, payload.len() as u32, 1)
                ),
                &retired
            )
            .unwrap()
            .code,
        0
    );
    assert_eq!(
        store
            .execute_control_import(
                "alice",
                &command(
                    &authority,
                    "c0",
                    2,
                    chunk_action(&payload, payload.len() as u32, 0)
                )
            )
            .unwrap()
            .code,
        0
    );
    let committing = {
        let store = store.clone();
        let authority = authority.clone();
        std::thread::spawn(move || {
            store
                .execute_control_import(
                    "alice",
                    &command(
                        &authority,
                        "commit",
                        3,
                        ImportAction::Commit(CommitControlImport {}),
                    ),
                )
                .unwrap()
        })
    };
    let recovering = {
        let store = store.clone();
        let authority = authority.clone();
        std::thread::spawn(move || {
            store
                .execute_control_import("bob", &recover(&authority, "bob-recover", 3, 1))
                .unwrap()
        })
    };
    let commit = committing.join().unwrap();
    let recovery = recovering.join().unwrap();
    let codes = [commit.code, recovery.code];
    assert!(
        codes.contains(&0) && codes.contains(&(Code::FailedPrecondition as u32)),
        "exactly one terminal step wins: {codes:?}"
    );
    let workflow = store
        .control_import_workflow("bob", &key("books"), b"import-1")
        .unwrap();
    if commit.code == 0 {
        assert_eq!(workflow.phase, ControlImportPhase::Committed as i32);
        assert!(store.control_snapshot("bob", &key("books")).is_ok());
    } else {
        assert_eq!(workflow.phase, ControlImportPhase::Recovered as i32);
        assert_eq!(
            store
                .control_snapshot("bob", &key("books"))
                .err()
                .unwrap()
                .code(),
            Code::NotFound
        );
    }
    assert_eq!(
        (workflow.reserved_bytes, workflow.reserved_decisions),
        (0, 0)
    );
    drop(store);
    SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
}

#[test]
fn recovery_exit_worker() {
    let Some(mode) = std::env::var_os("PSEARCH_IMPORT_RECOVERY_EXIT_FAULT") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("PSEARCH_IMPORT_RECOVERY_DIR").unwrap());
    let limits = limits(64 << 10, 64);
    let authority = identity(7);
    let store =
        SourceAuthorityStore::create(&root.join("authority.redb"), &authority, &policy(), &limits)
            .unwrap();
    let legacy = DurableControlPlane::open_existing(
        root.join("legacy.json"),
        test_fixtures::populated_policy(),
    )
    .unwrap()
    .with_collection("books")
    .unwrap();
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let payload = payload(&retired, supplement(&checkpoint));
    store
        .begin_control_import(
            "alice",
            &command(
                &authority,
                "begin",
                1,
                begin_action(&retired, &payload, payload.len() as u32, 1),
            ),
            &retired,
        )
        .unwrap();
    store
        .execute_control_import(
            "alice",
            &command(
                &authority,
                "c0",
                2,
                chunk_action(&payload, payload.len() as u32, 0),
            ),
        )
        .unwrap();
    *store.inner.fault.lock().unwrap() = Some(match mode.to_str().unwrap() {
        "before" => Fault::ExitBeforeCommit,
        "after" => Fault::ExitAfterCommit,
        other => panic!("unknown exit fault {other}"),
    });
    store
        .execute_control_import("bob", &recover(&authority, "bob-recover", 3, 1))
        .unwrap();
    panic!("exit fault did not terminate the worker");
}

#[test]
fn abrupt_exit_around_the_recovery_commit_leaves_staging_or_recovered() {
    for fault in ["before", "after"] {
        let dir = Directory::new(&format!("recovery-exit-{fault}"));
        std::fs::write(
            dir.legacy(),
            test_fixtures::populated_state_json("books", 2),
        )
        .unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("source_authority::import_tests::recovery_exit_worker")
            .arg("--nocapture")
            .env("PSEARCH_IMPORT_RECOVERY_EXIT_FAULT", fault)
            .env("PSEARCH_IMPORT_RECOVERY_DIR", &dir.0)
            .status_guarded()
            .unwrap();
        assert_eq!(status.code(), Some(87), "{fault}");
        let authority = identity(7);
        let store = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
        let workflow = store
            .control_import_workflow("bob", &key("books"), b"import-1")
            .unwrap();
        if fault == "before" {
            assert_eq!(workflow.phase, ControlImportPhase::Staging as i32);
            assert!(workflow.reserved_bytes > 0);
            assert_eq!(
                store
                    .control_import_decision("bob", &key("books"), b"bob-recover")
                    .err()
                    .unwrap()
                    .code(),
                Code::NotFound
            );
        } else {
            assert_eq!(workflow.phase, ControlImportPhase::Recovered as i32);
            assert_eq!(workflow.reserved_bytes, 0);
        }
        let decision = store
            .execute_control_import("bob", &recover(&authority, "bob-recover", 3, 1))
            .unwrap();
        assert_eq!(decision.code, 0, "{fault}: {}", decision.message);
        assert_eq!(decision.control_revision, 4);
        assert_eq!(
            store
                .control_import_workflow("bob", &key("books"), b"import-1")
                .unwrap()
                .phase,
            ControlImportPhase::Recovered as i32
        );
        drop(store);
        SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    }
}
