//! Shared kit for the control-authority adversarial suite
//! (`docs/control-authority-test-harness.md`).
//!
//! Everything here mirrors the in-crate `src/source_authority/import_tests.rs`
//! helpers through the public API only: same fixtures, same command shapes,
//! same revisions. No new dependencies; temp dirs and process control are
//! hand-rolled (`tests/multiprocess.rs` idiom).
//!
//! Shared between the `control_authority_adversarial` and
//! `control_authority_model` targets, each of which uses a subset.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pipestream_search::capacity_tiers::{
    plan_tiers, FragmentRequest, PartitionIdentity, TierSnapshot,
};
use pipestream_search::control_plane::{
    ClusterControlService, ControlPolicy, DurableControlPlane, LegacyControlCheckpoint,
    RetiredLegacyControl,
};
use pipestream_search::coordinator::TopologyRoute;
use pipestream_search::pb::cluster_control_server::ClusterControl;
use pipestream_search::pb::storage::capacity_transition::Action as CapacityAction;
use pipestream_search::pb::storage::control_import_command::Action as ImportAction;
use pipestream_search::pb::storage::legacy_control_import_supplement::{
    Derived, Placement, Provider,
};
use pipestream_search::pb::storage::source_authority_command::Action;
use pipestream_search::pb::storage::{
    AbortControlImport, ActivateSourceOwner, BeginControlImport, CancelPreparedSourceOwner,
    CapacityConfiguration, CapacityConfigureCommand, CapacityObservation, CapacityPartition,
    CapacityResidency, CapacityShardRef, CapacityTierPolicy, CapacityTierSpec, CapacityTransition,
    CommitCapacityExpiry, CommitControlImport, ConfirmSourceOwnerReady, ControlImportChunk,
    ControlImportCommand, ControlImportPayload, ControlImportReceipt, ControlPlannerPolicy,
    ControlProviderGeometry, LegacyControlImportSupplement, LegacyControlRetirementRequest,
    LogicalSourceOwner, PlacementRouteCode, RecoverControlImport, RegisterCapacityReporter,
    ReplaceSourceCollectionGrants, SourceAuthorityCommand, SourceAuthorityIdentity,
    SourceAuthorityLimits, SourceOwnerCompletion, SourceResidency, SourceStorageTarget,
};
use pipestream_search::pb::{
    AccessAction, AccessPolicy, CollectionGrant, CollectionResource, NodeCapacity, NodeLease,
    NodeResidency, PlacementNode, PlacementTree, RegisterNodeRequest, ReportShardRequest,
    ShardReplicaRole, ShardReplicaState,
};
use pipestream_search::sha256;
use pipestream_search::source_authority::{
    chunk_digest, payload_digest, retirement_digest, PlanningContext, SourceAuthorityStore,
};
use prost::Message;
use tonic::Request;

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
    import_command_policy(authority, id, control_revision, 1, workflow, action)
}

/// The import-command builder with an explicit expected policy revision
/// (the fixed-suite [`import_command`] pins 1, the bootstrap revision).
pub fn import_command_policy(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control_revision: u64,
    policy_revision: u64,
    workflow: &[u8],
    action: ImportAction,
) -> ControlImportCommand {
    ControlImportCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key()),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: policy_revision,
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

/// Administrative recovery: any current Admin may terminate a staging
/// workflow (docs/control-import.md, "Administrative recovery").
pub fn recover_action() -> ImportAction {
    ImportAction::Recover(RecoverControlImport {})
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

pub fn cancel_action(workflow: &[u8]) -> Action {
    Action::Cancel(CancelPreparedSourceOwner {
        workflow_id: workflow.to_vec(),
    })
}

/// Activation of a prepared owner (docs/source-owner-admission.md,
/// "Activation: the committed fence"); from tests only the refusal paths
/// are reachable, since READY requires the pub(crate) readiness proof.
pub fn activate_action(workflow: &[u8]) -> Action {
    Action::Activate(ActivateSourceOwner {
        workflow_id: workflow.to_vec(),
    })
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

// ---- SIGKILL import worker --------------------------------------------------

/// How long an armed worker waits for the parent's `go` file at each
/// durable step before exiting cleanly (which fails the parent's kill
/// assertion loudly).
pub const WORKER_GO_TIMEOUT: Duration = Duration::from_secs(120);

/// Worker body for the SIGKILL import-recovery tests
/// (`control_authority_adversarial::sigkill_recovery_at_every_import_boundary`
/// and `control_authority_planner::plan_digest_stable_across_sigkill_recovery`).
/// Builds the same deterministic state as the parent's baseline, writes
/// marker `mk` after each durable step, and when armed at `k` waits (up to
/// [`WORKER_GO_TIMEOUT`]) for a `go` file so the parent controls the exact
/// kill point. Early-returns unless [`WORKER_ENV`] is set.
/// Staged import with the slice-1 marker schedule: begin (m1), one marker
/// after each chunk (m2..m4), commit (m5). Shared by both SIGKILL worker
/// bodies; the parent arms one marker and controls the kill point with a
/// `go` file.
fn import_with_markers(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    retired: &RetiredLegacyControl,
    payload: &[u8],
    dir: &TestDir,
    arm: u32,
) {
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
    pause_for_go(dir, arm, 1, WORKER_GO_TIMEOUT);
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
        pause_for_go(dir, arm, 2 + ordinal, WORKER_GO_TIMEOUT);
    }
    let commit = store
        .execute_control_import(
            "alice",
            &import_command(authority, "commit", revision, WORKFLOW, commit_action()),
        )
        .unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
    pause_for_go(dir, arm, 5, WORKER_GO_TIMEOUT);
}

pub fn sigkill_import_worker_body() {
    if std::env::var_os(WORKER_ENV).is_none() {
        return;
    }
    let dir = TestDir::from_env();
    let arm: u32 = std::env::var(WORKER_ARM_ENV)
        .unwrap()
        .parse()
        .expect("PSEARCH_ADV_ARM is a marker index");
    let (store, authority) = create_store(&dir);
    let legacy = legacy_plane(&dir, 2);
    let (retired, request) = retire(&store, &legacy, 1);
    std::fs::write(dir.request(), request.encode_to_vec()).unwrap();
    pause_for_go(&dir, arm, 0, WORKER_GO_TIMEOUT);

    let payload = import_payload(&retired);
    import_with_markers(&store, &authority, &retired, &payload, &dir, arm);
}

// ---- capacity-planner leg (slice 3b: the persisted adapter) -----------------

/// Cohort length shared by every capacity configuration in this suite; the
/// observation windows are `[PLANNING_INSTANT_UNIX_MS - CAPACITY_COHORT_MS,
/// PLANNING_INSTANT_UNIX_MS)`, aligned to phase 0.
pub const CAPACITY_COHORT_MS: u64 = 600_000;
/// Fixed planning instant: twenty cohorts past the epoch at phase 0. Node
/// eligibility compares the committed lease expiry against this instant; the
/// imported leases were captured at wall-clock registration time, so they
/// always win and the instant stays deterministic.
pub const PLANNING_INSTANT_UNIX_MS: u64 = 20 * CAPACITY_COHORT_MS;
/// Generation every reported shard replica carries; the adapter names the
/// primary's generation the shard's source generation.
pub const SOURCE_GENERATION: u64 = 7;
pub const NODE_A: &str = "node-a";
pub const NODE_B: &str = "node-b";
pub const SHARD_A: &str = "shard-a";
pub const SHARD_B: &str = "shard-b";
/// Process incarnations the two fixture reporters register with.
pub const INC_A: u8 = 0x0a;
pub const INC_B: u8 = 0x0b;

/// The placement tree of the in-crate capacity fixtures: leaf "old" catches
/// rows with year < 2020, leaf "rest" everything else.
pub fn placement_tree() -> PlacementTree {
    PlacementTree {
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
    }
}

fn leaf_code(name: &str) -> u64 {
    let placement = pipestream_search::placement::Placement::validate(
        &pipestream_search::placement::PlacementTreeConfig::from_proto(&placement_tree()),
    )
    .unwrap();
    placement.leaf_by_name(name).unwrap().code as u64
}

/// The kit supplement plus the placement tree and one route code per route:
/// route 0 names leaf "old", route 1 names leaf "rest". The planner adapter
/// names a shard's leaf only when the import carried placement for the
/// route whose hash range exactly matches the shard's primary.
pub fn placement_supplement(checkpoint: &LegacyControlCheckpoint) -> LegacyControlImportSupplement {
    let mut supplement = supplement(checkpoint);
    supplement.placement = Some(Placement::Tree(placement_tree()));
    supplement.route_codes = ["old", "rest"]
        .map(|name| PlacementRouteCode {
            has_placement: true,
            placement: leaf_code(name),
        })
        .to_vec();
    supplement
}

/// Payload + placement supplement rebuilt from a (possibly recovered) holder.
pub fn import_payload_placement(retired: &RetiredLegacyControl) -> Vec<u8> {
    payload(retired, placement_supplement(&checkpoint_of(retired)))
}

/// A pristine 2-route legacy plane with two registered server nodes (rack-a,
/// rack-b) and one reported ready primary per route — shard-a on node-a in
/// route 0, shard-b on node-b in route 1 — exactly the committed shape the
/// capacity adapter derives shards, leaves, pools and eligible nodes from.
///
/// Registration and shard reports are cluster routes, not source-authority
/// commands, so they run through the public `ClusterControl` trait over a
/// short-lived in-process runtime; the plane is then reopened from disk so
/// the caller owns it for retirement. Lease duration is the plane maximum
/// (300s) and is plenty: eligibility is judged at the fixed planning
/// instant of 1970, long before any wall-clock expiry.
pub fn legacy_plane_with_cluster(dir: &TestDir) -> DurableControlPlane {
    let plane = legacy_plane(dir, 2);
    let control = ClusterControlService::new(plane);
    let runtime = tokio::runtime::Runtime::new().expect("cluster control runtime");
    let register = |node_id: &str, port: u16, domain: &str| RegisterNodeRequest {
        collection: COLLECTION.into(),
        node_id: node_id.into(),
        addr: format!("127.0.0.1:{port}"),
        capacity: Some(NodeCapacity {
            disk_bytes: 26_843_545_600,
            used_disk_bytes: 0,
            failure_domain: domain.into(),
            residency: NodeResidency::Server as i32,
            ..Default::default()
        }),
        lease_ms: 300_000,
    };
    let lease_a = runtime
        .block_on(ClusterControl::register_node(
            &control,
            Request::new(register(NODE_A, 9_101, "rack-a")),
        ))
        .unwrap()
        .into_inner();
    let lease_b = runtime
        .block_on(ClusterControl::register_node(
            &control,
            Request::new(register(NODE_B, 9_102, "rack-b")),
        ))
        .unwrap()
        .into_inner();
    // One primary per route; the reported ranges exactly tile the two
    // routes of `legacy_plane`, which is how derive() names each shard's
    // leaf through its route's committed placement code.
    let space = u64::MAX as u128 + 1;
    let report = |lease: &NodeLease, shard: &str, route: usize| ReportShardRequest {
        collection: COLLECTION.into(),
        node_id: lease.node_id.clone(),
        lease_token: lease.lease_token,
        replica: Some(ShardReplicaState {
            collection: COLLECTION.into(),
            shard_id: shard.into(),
            node_id: lease.node_id.clone(),
            generation: SOURCE_GENERATION,
            hash_lo: (route as u128 * space / 2) as u64,
            hash_hi: ((route as u128 + 1) * space / 2 - 1) as u64,
            rows: 1_000_000,
            bytes: 268_435_456,
            role: ShardReplicaRole::Primary as i32,
            ready: true,
            ..Default::default()
        }),
    };
    for (lease, shard, route) in [(lease_a, SHARD_A, 0usize), (lease_b, SHARD_B, 1usize)] {
        runtime
            .block_on(ClusterControl::report_shard(
                &control,
                Request::new(report(&lease, shard, route)),
            ))
            .unwrap();
    }
    drop(control);
    DurableControlPlane::open_existing(dir.legacy(), control_policy()).unwrap()
}

fn capacity_tier(name: &str, lo: u64, hi: u64, warmth: u64) -> CapacityTierSpec {
    CapacityTierSpec {
        name: name.into(),
        residency: CapacityResidency::Server as i32,
        min_replicas: 1,
        scans_per_byte_nanos_lo: lo,
        scans_per_byte_nanos_hi: hi,
        max_seconds_since_scan: warmth,
    }
}

/// The three-tier policy of the slice-2b fixture, rebased for single-replica
/// pools: every shard has exactly one committed copy, so every tier asks for
/// one replica. `hot_hi` lets the digest-move test change the policy bytes.
pub fn capacity_configuration(hot_hi: u64) -> CapacityConfiguration {
    CapacityConfiguration {
        format_version: 1,
        cohort_length_ms: CAPACITY_COHORT_MS,
        cohort_phase_unix_ms: 0,
        max_total_records: 64,
        max_total_bytes: 1 << 20,
        max_registered_nodes: 8,
        policy: Some(CapacityTierPolicy {
            format_version: 1,
            tiers: vec![
                capacity_tier("hot", 100, hot_hi, 0),
                capacity_tier("warm", 1, 100, 86_400),
                capacity_tier("archive", 0, 1, 0),
            ],
        }),
    }
}

pub fn configure_command(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control_revision: u64,
    policy_revision: u64,
    configuration: CapacityConfiguration,
) -> CapacityConfigureCommand {
    CapacityConfigureCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key()),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: policy_revision,
        configuration: Some(configuration),
    }
}

fn capacity_transition(
    authority: &SourceAuthorityIdentity,
    action: CapacityAction,
) -> CapacityTransition {
    CapacityTransition {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key()),
        action: Some(action),
    }
}

pub fn register_transition(
    authority: &SourceAuthorityIdentity,
    node: &str,
    incarnation: u8,
) -> CapacityTransition {
    capacity_transition(
        authority,
        CapacityAction::Register(RegisterCapacityReporter {
            node_id: node.into(),
            process_incarnation: vec![incarnation; 16],
        }),
    )
}

pub fn report_transition(
    authority: &SourceAuthorityIdentity,
    observation: CapacityObservation,
) -> CapacityTransition {
    capacity_transition(authority, CapacityAction::Report(observation))
}

pub fn expire_transition(
    authority: &SourceAuthorityIdentity,
    now_unix_ms: u64,
    max_age_ms: u64,
) -> CapacityTransition {
    capacity_transition(
        authority,
        CapacityAction::Expire(CommitCapacityExpiry {
            now_unix_ms,
            max_age_ms,
        }),
    )
}

fn observation(
    node: &str,
    incarnation: u8,
    leaf: &str,
    shard: &str,
    rows: u64,
    resident_bytes: u64,
    scans: u64,
    scan_bytes: u64,
    queue_wait_p50_us: u64,
    queue_wait_p99_us: u64,
    samples: u32,
    last_scanned_unix_ms: u64,
    topology_generation: u64,
) -> CapacityObservation {
    CapacityObservation {
        format_version: 1,
        partition: Some(CapacityPartition {
            workspace: WORKSPACE.into(),
            collection: COLLECTION.into(),
            derived_fingerprint: vec![0; 32],
            column: "year_bucket".into(),
            bucket: 7,
        }),
        node_id: node.into(),
        process_incarnation: vec![incarnation; 16],
        shard: Some(CapacityShardRef {
            shard: shard.into(),
            source_generation: SOURCE_GENERATION,
            storage_incarnation: vec![0; 16],
            ownership_epoch: 0,
        }),
        topology_generation,
        leaf: leaf.into(),
        rows,
        resident_bytes,
        scans_observed: scans,
        scan_bytes,
        queue_wait_p50_us,
        queue_wait_p99_us,
        samples,
        last_scanned_unix_ms,
        window_start_unix_ms: PLANNING_INSTANT_UNIX_MS - CAPACITY_COHORT_MS,
        window_end_unix_ms: PLANNING_INSTANT_UNIX_MS,
    }
}

/// The topology generation the committed control state carries after the
/// import: each legacy `report_shard` reconciles and publishes, so the
/// generation is whatever the plane published, read back from the snapshot
/// rather than assumed.
pub fn committed_generation(store: &SourceAuthorityStore) -> u64 {
    let snapshot = store.control_snapshot("alice", &key()).unwrap();
    snapshot
        .state
        .as_ref()
        .expect("imported snapshot carries collection state")
        .topology_generation
}

/// shard-a's owner report: scanned heavily inside the current window, the
/// "hot" shape of the slice-2b fixture (3,000,000 scans over 786,432,000,000
/// bytes ≈ 3,800 ns/byte).
pub fn scanned_observation(
    node: &str,
    incarnation: u8,
    topology_generation: u64,
) -> CapacityObservation {
    observation(
        node,
        incarnation,
        "old",
        SHARD_A,
        1_000_000,
        268_435_456,
        3_000_000,
        786_432_000_000,
        1_200,
        9_400,
        3_000,
        PLANNING_INSTANT_UNIX_MS - 5_000,
        topology_generation,
    )
}

/// shard-b's owner report: present but silent, the "archive" shape.
pub fn silent_observation(
    node: &str,
    incarnation: u8,
    topology_generation: u64,
) -> CapacityObservation {
    observation(
        node,
        incarnation,
        "rest",
        SHARD_B,
        500_000,
        134_217_728,
        0,
        0,
        0,
        0,
        0,
        0,
        topology_generation,
    )
}

/// The planning context at `instant`: both fixture leaves of bucket 7, bound
/// to the suite's resource triple and the committed topology generation.
pub fn planning_context(instant: u64, topology_generation: u64) -> PlanningContext {
    let partition = PartitionIdentity {
        workspace: WORKSPACE.into(),
        collection: COLLECTION.into(),
        derived_fingerprint: [0; 32],
        column: "year_bucket".into(),
        bucket: 7,
    };
    PlanningContext {
        planning_instant_unix_ms: instant,
        max_moves: 16,
        max_observation_age_ms: 10 * CAPACITY_COHORT_MS,
        clock_skew_bound_ms: 5_000,
        fragment_requests: ["old", "rest"]
            .map(|leaf| FragmentRequest {
                partition: partition.clone(),
                topology_generation,
                leaf: leaf.into(),
            })
            .to_vec(),
    }
}

/// The planner's view of the store through the real persisted adapter: the
/// input's debug form, the validated snapshot's plan digest, and the plan's
/// canonical bytes. Panics unless the adapter input validates and plans.
pub fn planner_view(store: &SourceAuthorityStore, instant: u64) -> (String, [u8; 32], Vec<u8>) {
    let input = store
        .planner_input(
            "alice",
            &key(),
            &planning_context(instant, committed_generation(store)),
        )
        .unwrap();
    let rendered = format!("{input:?}");
    let snapshot = TierSnapshot::validated(input).expect("adapter input must validate");
    let plan = plan_tiers(&snapshot).expect("plan_tiers must accept the adapter input");
    (rendered, snapshot.plan_digest(), plan.canonical_bytes())
}

/// A store that went through the full adapter flow: the cluster plane
/// (registered nodes, reported primaries) retired and imported with the
/// placement supplement, capacity configured at control revision 6, both
/// reporters registered, and one report per shard in the current cohort
/// window. Observation epoch is 2.
pub struct AdapterFixture {
    pub dir: TestDir,
    pub authority: SourceAuthorityIdentity,
    pub store: SourceAuthorityStore,
    /// The committed topology generation the reports and context bind to.
    pub generation: u64,
}

pub fn adapter_fixture(name: &str) -> AdapterFixture {
    adapter_fixture_staged(name, false)
}

/// [`adapter_fixture`], with the chunk staging order reversed when `reverse`
/// is set (the protocol keys chunks by ordinal and accepts any order).
pub fn adapter_fixture_staged(name: &str, reverse: bool) -> AdapterFixture {
    let dir = TestDir::new(name);
    let (store, authority) = create_store(&dir);
    let legacy = legacy_plane_with_cluster(&dir);
    let (retired, _request) = retire(&store, &legacy, 1);
    let payload = import_payload_placement(&retired);
    run_import_staged(&store, &authority, &retired, &payload, reverse);
    let generation = committed_generation(&store);
    // The import commits at control revision 6 (begin 1, three chunks,
    // commit); configure is the next retained command.
    let decision = store
        .configure_capacity(
            "alice",
            &configure_command(
                &authority,
                "cfg",
                6,
                1,
                capacity_configuration(1_000_000_000_000),
            ),
        )
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    for (node, incarnation) in [(NODE_A, INC_A), (NODE_B, INC_B)] {
        store
            .capacity_transition("alice", &register_transition(&authority, node, incarnation))
            .unwrap();
    }
    store
        .capacity_transition(
            "alice",
            &report_transition(&authority, scanned_observation(NODE_A, INC_A, generation)),
        )
        .unwrap();
    store
        .capacity_transition(
            "alice",
            &report_transition(&authority, silent_observation(NODE_B, INC_B, generation)),
        )
        .unwrap();
    AdapterFixture {
        dir,
        authority,
        store,
        generation,
    }
}

/// Begin -> three chunks -> commit; `reverse` stages the chunk ordinals in
/// reverse order.
pub fn run_import_staged(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    retired: &RetiredLegacyControl,
    payload: &[u8],
    reverse: bool,
) {
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
    let ordinals: Vec<u32> = if reverse {
        (0..chunk_count).rev().collect()
    } else {
        (0..chunk_count).collect()
    };
    let mut revision = 2u64;
    for ordinal in ordinals {
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
        assert_eq!(decision.code, 0, "chunk {ordinal}: {}", decision.message);
        revision += 1;
    }
    let commit = store
        .execute_control_import(
            "alice",
            &import_command(authority, "commit", revision, WORKFLOW, commit_action()),
        )
        .unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
}

/// Worker body for the adapter SIGKILL-recovery test
/// (`control_authority_planner::plan_digest_stable_across_sigkill_recovery`).
/// Identical durable steps to the parent's baseline — cluster plane,
/// retirement, placement import — on the slice-1 marker schedule. The parent
/// recovers the import and then runs the capacity steps itself.
/// Early-returns unless [`WORKER_ENV`] is set.
pub fn sigkill_capacity_worker_body() {
    if std::env::var_os(WORKER_ENV).is_none() {
        return;
    }
    let dir = TestDir::from_env();
    let arm: u32 = std::env::var(WORKER_ARM_ENV)
        .unwrap()
        .parse()
        .expect("PSEARCH_ADV_ARM is a marker index");
    let (store, authority) = create_store(&dir);
    let legacy = legacy_plane_with_cluster(&dir);
    let (retired, request) = retire(&store, &legacy, 1);
    std::fs::write(dir.request(), request.encode_to_vec()).unwrap();
    pause_for_go(&dir, arm, 0, WORKER_GO_TIMEOUT);

    let payload = import_payload_placement(&retired);
    import_with_markers(&store, &authority, &retired, &payload, &dir, arm);
}

/// A Prepare action carrying the caller's catalog history id (the
/// managed-catalog bridge binds the preparation to that history).
pub fn prepare_action_history(workflow: &[u8], history_id: Vec<u8>) -> Action {
    Action::Prepare(pipestream_search::pb::storage::PrepareSourceOwner {
        workflow_id: workflow.to_vec(),
        target: Some(SourceStorageTarget {
            node_id: "server-a".into(),
            storage_incarnation: vec![41; 16],
            history_id,
            residency: SourceResidency::Server as i32,
            resident_device_id: String::new(),
        }),
    })
}
