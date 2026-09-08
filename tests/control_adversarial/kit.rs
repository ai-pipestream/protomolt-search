//! Shared kit for the control-authority adversarial suite
//! (`docs/control-authority-test-harness.md`).
//!
//! Everything here mirrors the in-crate `src/source_authority/import_tests.rs`
//! helpers through the public API only: same fixtures, same command shapes,
//! same revisions. No new dependencies; temp dirs and process control are
//! hand-rolled (`tests/multiprocess.rs` idiom).

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pipestream_search::control_plane::{
    ControlPolicy, DurableControlPlane, LegacyControlCheckpoint, RetiredLegacyControl,
};
use pipestream_search::coordinator::TopologyRoute;
use pipestream_search::pb::storage::control_import_command::Action as ImportAction;
use pipestream_search::pb::storage::legacy_control_import_supplement::{
    Derived, Placement, Provider,
};
use pipestream_search::pb::storage::source_authority_command::Action;
use pipestream_search::pb::storage::{
    AbortControlImport, BeginControlImport, CommitControlImport, ConfirmSourceOwnerReady,
    ControlImportChunk, ControlImportCommand, ControlImportPayload, ControlImportReceipt,
    ControlPlannerPolicy, ControlProviderGeometry, LegacyControlImportSupplement,
    LegacyControlRetirementRequest, LogicalSourceOwner, ReplaceSourceCollectionGrants,
    SourceAuthorityCommand, SourceAuthorityIdentity, SourceAuthorityLimits, SourceOwnerCompletion,
    SourceResidency, SourceStorageTarget,
};
use pipestream_search::pb::{AccessAction, AccessPolicy, CollectionGrant, CollectionResource};
use pipestream_search::sha256;
use pipestream_search::source_authority::{
    chunk_digest, payload_digest, retirement_digest, SourceAuthorityStore,
};
use prost::Message;

pub const WORKER_ENV: &str = "PSEARCH_ADV_WORKER";
pub const WORKER_DIR_ENV: &str = "PSEARCH_ADV_DIR";
pub const WORKER_ARM_ENV: &str = "PSEARCH_ADV_ARM";
pub const WORKSPACE: &str = "test";
pub const COLLECTION: &str = "books";
pub const WORKFLOW: &[u8] = b"import-1";
/// Number of chunks every import in this suite stages.
pub const CHUNKS: u32 = 3;
/// Seed shared by every store in the suite so digests are comparable.
pub const SEED: u8 = 7;

/// Hand-rolled temp directory; removed on drop.
pub struct TestDir(PathBuf);

impl TestDir {
    pub fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "control-adversarial-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    /// The directory named by `PSEARCH_ADV_DIR` (the worker side).
    pub fn from_env() -> Self {
        let path = PathBuf::from(std::env::var_os(WORKER_DIR_ENV).unwrap());
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn authority(&self) -> PathBuf {
        self.0.join("authority.redb")
    }

    pub fn legacy(&self) -> PathBuf {
        self.0.join("legacy.json")
    }

    pub fn request(&self) -> PathBuf {
        self.0.join("request.bin")
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn identity(seed: u8) -> SourceAuthorityIdentity {
    SourceAuthorityIdentity {
        format_version: 1,
        group_id: vec![seed; 16],
        authority_incarnation: vec![seed.wrapping_add(1); 16],
    }
}

pub fn grant(principal: &str, collection: &str) -> CollectionGrant {
    CollectionGrant {
        principal: principal.into(),
        workspace: WORKSPACE.into(),
        collection: collection.into(),
        actions: vec![AccessAction::Admin as i32],
        ..Default::default()
    }
}

/// alice and bob are Admins of `test/books`.
pub fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 1,
        revision: 1,
        resources: [CollectionResource {
            workspace: WORKSPACE.into(),
            collection: COLLECTION.into(),
        }]
        .to_vec(),
        grants: vec![grant("alice", COLLECTION), grant("bob", COLLECTION)],
    }
}

/// The collection resource key (empty owner), used by policy commands and imports.
pub fn key() -> LogicalSourceOwner {
    LogicalSourceOwner {
        workspace: WORKSPACE.into(),
        collection: COLLECTION.into(),
        owner_id: Vec::new(),
    }
}

/// A logical owner key for Prepare/Confirm commands.
pub fn owner_key() -> LogicalSourceOwner {
    LogicalSourceOwner {
        owner_id: b"phone-owner".to_vec(),
        ..key()
    }
}

pub fn limits() -> SourceAuthorityLimits {
    SourceAuthorityLimits {
        max_owners: 16,
        max_decisions: 128,
        max_payload_bytes: 64 << 20,
        max_command_bytes: 8 << 10,
    }
}

pub fn control_policy() -> ControlPolicy {
    ControlPolicy {
        lease_ms: 31_337,
        replication_factor: 3,
        split_rows: 123_456,
        merge_rows: 7_654,
        compact_segments: 11,
        compact_tombstone_ppm: 222_333,
        history_limit: 17,
    }
}

/// The suite's canonical store, seeded so every test starts identical.
pub fn create_store(dir: &TestDir) -> (SourceAuthorityStore, SourceAuthorityIdentity) {
    let authority = identity(SEED);
    let store =
        SourceAuthorityStore::create(&dir.authority(), &authority, &policy(), &limits()).unwrap();
    (store, authority)
}

pub fn open_store(dir: &TestDir, authority: &SourceAuthorityIdentity) -> SourceAuthorityStore {
    SourceAuthorityStore::open(&dir.authority(), authority).unwrap()
}

/// A pristine durable legacy plane with `routes` current routes bootstrapped
/// at generation 1; the routes tile the u64 hash space in order.
pub fn legacy_plane(dir: &TestDir, routes: usize) -> DurableControlPlane {
    let plane = DurableControlPlane::open(dir.legacy(), control_policy())
        .unwrap()
        .with_collection(COLLECTION)
        .unwrap();
    let space = u64::MAX as u128 + 1;
    let routes: Vec<TopologyRoute> = (0..routes)
        .map(|index| {
            let lo = (index as u128 * space / routes as u128) as u64;
            let hi = ((index as u128 + 1) * space / routes as u128 - 1) as u64;
            TopologyRoute {
                addr: format!("127.0.0.1:{}", 9_000 + index),
                replica: None,
                hash_range: Some((lo, hi)),
                placement: None,
            }
        })
        .collect();
    plane.bootstrap_topology(1, &routes).unwrap();
    plane
}

pub fn retirement_request(checkpoint: &[u8], command_id: &[u8]) -> LegacyControlRetirementRequest {
    LegacyControlRetirementRequest {
        format_version: 1,
        key: Some(key()),
        command_id: command_id.to_vec(),
        expected_checkpoint_sha256: sha256::digest(checkpoint).to_vec(),
        expected_control_revision: 1,
        expected_policy_revision: 1,
    }
}

/// Retire `plane` under current Admin rights; returns the holder (keeps the
/// legacy lock) and the request for later recovery.
pub fn retire(
    store: &SourceAuthorityStore,
    plane: &DurableControlPlane,
    control_revision: u64,
) -> (RetiredLegacyControl, LegacyControlRetirementRequest) {
    let checkpoint = plane.checkpoint_for_import().unwrap();
    let mut request = retirement_request(&checkpoint, b"retire-books");
    request.expected_control_revision = control_revision;
    let retired = store
        .retire_legacy_control("alice", &request, plane)
        .unwrap();
    (retired, request)
}

pub fn checkpoint_of(retired: &RetiredLegacyControl) -> LegacyControlCheckpoint {
    LegacyControlCheckpoint::decode(retired.checkpoint_bytes()).unwrap()
}

fn geometry() -> ControlProviderGeometry {
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

/// The minimal valid supplement for a 2-route plane without placement trees
/// or derived columns (mirrors `import_tests.rs`).
pub fn supplement(checkpoint: &LegacyControlCheckpoint) -> LegacyControlImportSupplement {
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
            control: Some(checkpoint.policy().clone()),
        }),
        history_placement_unavailable: true,
    }
}

pub fn payload(
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

/// Payload + supplement rebuilt from a (possibly recovered) holder.
pub fn import_payload(retired: &RetiredLegacyControl) -> Vec<u8> {
    payload(retired, supplement(&checkpoint_of(retired)))
}

/// Declare exactly [`CHUNKS`] chunks: `chunk_bytes = ceil(payload / CHUNKS)`.
/// The protocol requires only `chunk_bytes <= chunk_capacity` and
/// `chunk_count = ceil(payload_bytes / chunk_bytes)`, so this splits even the
/// small 2-route payload into three chunks.
pub fn plan_three_chunks(payload_bytes: usize) -> (u32, u32) {
    let chunk_bytes = payload_bytes.div_ceil(CHUNKS as usize) as u32;
    assert!(chunk_bytes > 0, "payload must be non-empty");
    (chunk_bytes, CHUNKS)
}

pub fn import_command(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control_revision: u64,
    workflow: &[u8],
    action: ImportAction,
) -> ControlImportCommand {
    ControlImportCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key()),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: 1,
        workflow_id: workflow.to_vec(),
        action: Some(action),
    }
}

pub fn begin_action(
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

pub fn chunk_action(payload: &[u8], chunk_bytes: u32, ordinal: u32) -> ImportAction {
    let start = ordinal as usize * chunk_bytes as usize;
    let end = payload.len().min(start + chunk_bytes as usize);
    ImportAction::Chunk(ControlImportChunk {
        ordinal,
        bytes: payload[start..end].to_vec(),
        sha256: chunk_digest(&payload[start..end]),
    })
}

pub fn commit_action() -> ImportAction {
    ImportAction::Commit(CommitControlImport {})
}

pub fn abort_action() -> ImportAction {
    ImportAction::Abort(AbortControlImport {})
}

/// Begin at control revision 1, stage every chunk, commit; returns the receipt.
pub fn run_import(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    retired: &RetiredLegacyControl,
    payload: &[u8],
) -> (ControlImportReceipt, u32) {
    let (chunk_bytes, chunk_count) = plan_three_chunks(payload.len());
    let begin = store
        .begin_control_import(
            "alice",
            &import_command(
                authority,
                "begin",
                1,
                WORKFLOW,
                begin_action(retired, payload, chunk_bytes, chunk_count),
            ),
            retired,
        )
        .unwrap();
    assert_eq!(begin.code, 0, "{}", begin.message);
    let mut revision = 2u64;
    for ordinal in 0..chunk_count {
        let decision = store
            .execute_control_import(
                "alice",
                &import_command(
                    authority,
                    &format!("chunk-{ordinal}"),
                    revision,
                    WORKFLOW,
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
            &import_command(authority, "commit", revision, WORKFLOW, commit_action()),
        )
        .unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
    (commit.receipt.unwrap(), chunk_count)
}

pub fn snapshot_digest(
    store: &SourceAuthorityStore,
    principal: &str,
    key: &LogicalSourceOwner,
) -> [u8; 32] {
    let snapshot = store.control_snapshot(principal, key).unwrap();
    snapshot
        .digest
        .as_slice()
        .try_into()
        .expect("control snapshot digest is 32 bytes")
}

// ---- general (non-import) command builders -------------------------------

pub fn source_command(
    authority: &SourceAuthorityIdentity,
    key: &LogicalSourceOwner,
    id: &str,
    control_revision: u64,
    policy_revision: u64,
    generation: u64,
    action: Action,
) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key.clone()),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: policy_revision,
        expected_ownership_generation: generation,
        action: Some(action),
    }
}

pub fn prepare_action(workflow: &[u8]) -> Action {
    Action::Prepare(pipestream_search::pb::storage::PrepareSourceOwner {
        workflow_id: workflow.to_vec(),
        target: Some(SourceStorageTarget {
            node_id: "server-a".into(),
            storage_incarnation: vec![41; 16],
            history_id: vec![42; 16],
            residency: SourceResidency::Server as i32,
            resident_device_id: String::new(),
        }),
    })
}

pub fn replace_grants_action(grants: Vec<CollectionGrant>) -> Action {
    Action::ReplaceGrants(ReplaceSourceCollectionGrants { grants })
}

/// A syntactically complete ConfirmReady action; the general `execute` path
/// must refuse it with PermissionDenied before any of this is validated.
pub fn confirm_ready_action(workflow: &[u8]) -> Action {
    Action::ConfirmReady(ConfirmSourceOwnerReady {
        workflow_id: workflow.to_vec(),
        completion: Some(SourceOwnerCompletion {
            format_version: 1,
            binding_sha256: vec![7; 32],
            bound_at_sequence: 1,
            history_id: vec![42; 16],
            node_id: "server-a".into(),
            storage_incarnation: vec![41; 16],
        }),
    })
}

// ---- worker/parent process injection --------------------------------------

/// Spawn this same test binary running only `test_name` with `envs` set.
pub fn spawn_worker(test_name: &str, envs: &[(&str, &str)]) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .arg("--test-threads=1");
    for (name, value) in envs {
        command.env(name, value);
    }
    command.spawn().expect("spawn adversarial worker")
}

/// Kill-on-drop guard so a failed assertion never leaks a worker process.
pub struct KillOnDrop(pub Child);

impl KillOnDrop {
    pub fn child(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// `kill -9` the worker and assert it died by SIGKILL (i.e. the crash was
/// actually injected, not a clean exit).
pub fn kill9(child: &mut Child) {
    let pid = child.id().to_string();
    let status = Command::new("/bin/kill")
        .args(["-9", &pid])
        .status()
        .unwrap();
    assert!(status.success(), "kill -9 {pid} failed");
    let waited = child.wait().unwrap();
    use std::os::unix::process::ExitStatusExt as _;
    assert_eq!(
        waited.signal(),
        Some(9),
        "worker pid {pid} must die by SIGKILL, got {waited}"
    );
}

pub fn marker_path(dir: &TestDir, k: u32) -> PathBuf {
    dir.path().join(format!("m{k}"))
}

/// Write marker `mk`; when the worker is armed at `k`, wait for the parent's
/// `go` file (up to `timeout`) so the parent controls the exact kill point.
/// A timeout exits cleanly; the parent's SIGKILL assertion then fails loudly.
pub fn pause_for_go(dir: &TestDir, arm: u32, k: u32, timeout: Duration) {
    std::fs::write(marker_path(dir, k), b"").unwrap();
    if arm != k {
        return;
    }
    let deadline = Instant::now() + timeout;
    while !dir.path().join("go").exists() {
        if Instant::now() >= deadline {
            eprintln!("adversarial worker: no go file after {timeout:?}; exiting cleanly");
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Poll for marker `mk` up to `timeout`; fail loudly if it never appears.
pub fn wait_marker(dir: &TestDir, k: u32, timeout: Duration) {
    let marker = marker_path(dir, k);
    let deadline = Instant::now() + timeout;
    while !marker.exists() {
        assert!(
            Instant::now() < deadline,
            "marker m{k} never appeared in {}",
            dir.path().display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
