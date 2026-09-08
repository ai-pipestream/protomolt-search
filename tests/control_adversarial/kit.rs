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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use pipestream_search::capacity_tiers::{
    CapacityTier, CommittedCopy, CommittedShard, CommittedView, FragmentRequest, IngestOutcome,
    LeafCoverage, NodeCapacity, NodeRecord, NodeResidency, ObservationStore, PartitionIdentity,
    PartitionObservation, ReporterIdentity, ShardRef, TierPolicy, TierSnapshotInput,
};
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
    ControlCollectionSnapshot, ControlImportChunk, ControlImportCommand, ControlImportPayload,
    ControlImportReceipt, ControlPlannerPolicy, ControlProviderGeometry,
    LegacyControlImportSupplement, LegacyControlRetirementRequest, LogicalSourceOwner,
    ReplaceSourceCollectionGrants, SourceAuthorityCommand, SourceAuthorityIdentity,
    SourceAuthorityLimits, SourceOwnerCompletion, SourceResidency, SourceStorageTarget,
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
    let (chunk_bytes, chunk_count) = plan_three_chunks(payload.len());
    let begin = store
        .begin_control_import(
            "alice",
            &import_command(
                &authority,
                "begin",
                1,
                WORKFLOW,
                begin_action(&retired, &payload, chunk_bytes, chunk_count),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(begin.code, 0, "{}", begin.message);
    pause_for_go(&dir, arm, 1, WORKER_GO_TIMEOUT);
    let mut revision = 2u64;
    for ordinal in 0..chunk_count {
        let decision = store
            .execute_control_import(
                "alice",
                &import_command(
                    &authority,
                    &format!("chunk-{ordinal}"),
                    revision,
                    WORKFLOW,
                    chunk_action(&payload, chunk_bytes, ordinal),
                ),
            )
            .unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
        revision += 1;
        pause_for_go(&dir, arm, 2 + ordinal, WORKER_GO_TIMEOUT);
    }
    let commit = store
        .execute_control_import(
            "alice",
            &import_command(&authority, "commit", revision, WORKFLOW, commit_action()),
        )
        .unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
    pause_for_go(&dir, arm, 5, WORKER_GO_TIMEOUT);
}

// ---- capacity-planner leg (slice 2b) ----------------------------------------

/// Planning instant shared by every planner input in this suite: the fixed
/// fixture instant of the in-crate `capacity_tiers` tests.
pub const PLANNING_INSTANT_UNIX_MS: u64 = 1_788_868_800_000;
/// Cohort length shared by every observation store in this suite; the
/// observation windows are `[PLANNING_INSTANT_UNIX_MS - COHORT_LENGTH_MS,
/// PLANNING_INSTANT_UNIX_MS)`, aligned to phase 0.
pub const COHORT_LENGTH_MS: u64 = 600_000;

fn planner_inc(byte: u8) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[15] = byte;
    out
}

fn planner_tier(name: &str, min_replicas: u32, lo: u64, hi: u64, warmth: u64) -> CapacityTier {
    CapacityTier {
        name: name.to_string(),
        residency: NodeResidency::Server,
        min_replicas,
        scans_per_byte_nanos_lo: lo,
        scans_per_byte_nanos_hi: hi,
        max_seconds_since_scan: warmth,
    }
}

/// The three-tier policy of the in-crate planner fixtures.
pub fn planner_policy() -> TierPolicy {
    TierPolicy {
        tiers: vec![
            planner_tier("hot", 3, 100, 1_000_000_000_000, 0),
            planner_tier("warm", 2, 1, 100, 86_400),
            planner_tier("archive", 2, 0, 1, 0),
        ],
    }
}

/// A committed-copy spec, mirroring the in-crate fixture A copy specs.
pub struct PlannerCopySpec {
    pub node: &'static str,
    pub proc_inc: u8,
    pub stor_inc: u8,
    pub domain: &'static str,
}

pub const K1_S6: PlannerCopySpec = PlannerCopySpec {
    node: "krick-1",
    proc_inc: 0x0a,
    stor_inc: 0xa1,
    domain: "krick",
};
pub const P1_S6: PlannerCopySpec = PlannerCopySpec {
    node: "pi5v1",
    proc_inc: 0x0b,
    stor_inc: 0xb1,
    domain: "pi5-west",
};
pub const P3_S6: PlannerCopySpec = PlannerCopySpec {
    node: "pi5v3",
    proc_inc: 0x0c,
    stor_inc: 0xc1,
    domain: "pi5-east",
};
pub const P1_S7: PlannerCopySpec = PlannerCopySpec {
    node: "pi5v1",
    proc_inc: 0x0b,
    stor_inc: 0xb2,
    domain: "pi5-west",
};
pub const K1_S7: PlannerCopySpec = PlannerCopySpec {
    node: "krick-1",
    proc_inc: 0x0a,
    stor_inc: 0xa2,
    domain: "krick",
};

fn planner_committed_copy(spec: &PlannerCopySpec, coverage: [u8; 32]) -> CommittedCopy {
    CommittedCopy {
        node_id: spec.node.to_string(),
        storage_incarnation: planner_inc(spec.stor_inc),
        failure_domain: spec.domain.to_string(),
        complete: true,
        coverage_digest: coverage,
    }
}

/// Coverage digests of the in-crate fixtures; only the sharing rule matters
/// (every complete copy of one fragment carries one digest).
fn planner_cov_l4() -> [u8; 32] {
    sha256::digest(b"adversarial harness manifest: fragment (key_bucket=7, L4)")
}

fn planner_cov_l7() -> [u8; 32] {
    sha256::digest(b"adversarial harness manifest: fragment (key_bucket=7, L7)")
}

/// The committed view for the imported resource: the in-crate fixture A
/// shard/leaf/copies shape bound to the snapshot's resource triple and
/// topology generation. The imported 2-route plane registers no nodes, so
/// the coverage topology is harness-built, exactly as the in-crate planner
/// fixtures hand-build theirs.
pub fn planner_committed_view(snapshot: &ControlCollectionSnapshot) -> CommittedView {
    let state = snapshot
        .state
        .as_ref()
        .expect("imported snapshot carries collection state");
    let supplement = state
        .configuration
        .as_ref()
        .expect("imported snapshot carries the import supplement");
    let key = snapshot
        .key
        .as_ref()
        .expect("snapshot carries the resource key");
    let mut shards = BTreeMap::new();
    shards.insert(
        "s6".to_string(),
        CommittedShard {
            source_generation: 3,
            ownership_epoch: 5,
            leaves: BTreeMap::from([(
                "L4".to_string(),
                LeafCoverage {
                    pool: vec![
                        "krick-1".to_string(),
                        "pi5v1".to_string(),
                        "pi5v3".to_string(),
                    ],
                    owner_node: "krick-1".to_string(),
                    copies: vec![
                        planner_committed_copy(&K1_S6, planner_cov_l4()),
                        planner_committed_copy(&P1_S6, planner_cov_l4()),
                        planner_committed_copy(&P3_S6, planner_cov_l4()),
                    ],
                },
            )]),
        },
    );
    shards.insert(
        "s7".to_string(),
        CommittedShard {
            source_generation: 3,
            ownership_epoch: 2,
            leaves: BTreeMap::from([(
                "L7".to_string(),
                LeafCoverage {
                    pool: vec!["krick-1".to_string(), "pi5v1".to_string()],
                    owner_node: "pi5v1".to_string(),
                    copies: vec![
                        planner_committed_copy(&P1_S7, planner_cov_l7()),
                        planner_committed_copy(&K1_S7, planner_cov_l7()),
                    ],
                },
            )]),
        },
    );
    CommittedView {
        workspace: key.workspace.clone(),
        collection: key.collection.clone(),
        derived_fingerprint: sha256::digest(supplement.derived_fingerprint.as_bytes()),
        topology_generation: state.topology_generation,
        shards,
    }
}

fn planner_obs(
    view: &CommittedView,
    spec: &PlannerCopySpec,
    leaf: &str,
    shard: &str,
    sgen: u64,
    epoch: u64,
    rows: u64,
    bytes: u64,
    scans: u64,
    last: u64,
) -> PartitionObservation {
    PartitionObservation {
        partition: PartitionIdentity {
            workspace: view.workspace.clone(),
            collection: view.collection.clone(),
            derived_fingerprint: view.derived_fingerprint,
            column: "key_bucket".to_string(),
            bucket: 7,
        },
        reporter: ReporterIdentity {
            node_id: spec.node.to_string(),
            process_incarnation: planner_inc(spec.proc_inc),
        },
        shard: ShardRef {
            shard: shard.to_string(),
            source_generation: sgen,
            storage_incarnation: planner_inc(spec.stor_inc),
            ownership_epoch: epoch,
        },
        topology_generation: view.topology_generation,
        leaf: leaf.to_string(),
        rows,
        resident_bytes: bytes,
        scans_observed: scans,
        scan_bytes: if scans > 0 { 786_432_000_000 } else { 0 },
        queue_wait_p50_us: if scans > 0 { 1200 } else { 0 },
        queue_wait_p99_us: if scans > 0 { 9400 } else { 0 },
        samples: if scans > 0 { 3000 } else { 0 },
        last_scanned_unix_ms: last,
        window_start_unix_ms: PLANNING_INSTANT_UNIX_MS - COHORT_LENGTH_MS,
        window_end_unix_ms: PLANNING_INSTANT_UNIX_MS,
    }
}

/// The five fixture A reports, bound to `view`'s resource triple and
/// topology generation: the s6 owner scanned recently, every other copy
/// silent, and one stale scan on s7's owner so the archive tier has work.
pub fn planner_reports(view: &CommittedView) -> Vec<PartitionObservation> {
    vec![
        planner_obs(
            view,
            &K1_S6,
            "L4",
            "s6",
            3,
            5,
            1_000_000,
            268_435_456,
            3_000_000,
            PLANNING_INSTANT_UNIX_MS - 5000,
        ),
        planner_obs(view, &P1_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0),
        planner_obs(view, &P3_S6, "L4", "s6", 3, 5, 1_000_000, 268_435_456, 0, 0),
        planner_obs(
            view,
            &P1_S7,
            "L7",
            "s7",
            3,
            2,
            500_000,
            134_217_728,
            0,
            PLANNING_INSTANT_UNIX_MS - 3_600_000,
        ),
        planner_obs(view, &K1_S7, "L7", "s7", 3, 2, 500_000, 134_217_728, 0, 0),
    ]
}

/// A fresh observation store bound to `view`, with the three fixture node
/// incarnations registered (the same values as the in-crate `store_a`
/// fixture).
pub fn planner_observation_store(view: &CommittedView) -> ObservationStore {
    let mut store = ObservationStore::new(COHORT_LENGTH_MS, 0, 1000, 1 << 20, 64, view.clone())
        .expect("valid cohort config");
    store
        .register_incarnation("krick-1", planner_inc(0x0a))
        .unwrap();
    store
        .register_incarnation("pi5v1", planner_inc(0x0b))
        .unwrap();
    store
        .register_incarnation("pi5v3", planner_inc(0x0c))
        .unwrap();
    store
}

/// Feed `reports` into a fresh store (already-incarnation-registered) and
/// return it; `reverse` ingests in reverse key order to prove the stored
/// set, its digest, and the derived plan are order-insensitive.
pub fn feed_reports(view: &CommittedView, reverse: bool) -> ObservationStore {
    let mut store = planner_observation_store(view);
    let mut reports = planner_reports(view);
    if reverse {
        reports.reverse();
    }
    for report in reports {
        assert_eq!(store.ingest(report).expect("ingest"), IngestOutcome::Landed);
    }
    store
}

/// The three-node registry of the in-crate fixture: two eligible servers
/// with headroom and one with none.
pub fn planner_nodes() -> BTreeMap<String, NodeRecord> {
    let server = |domain: &str, total: u64| NodeRecord {
        residency: NodeResidency::Server,
        eligible: true,
        failure_domain: domain.to_string(),
        capacity: NodeCapacity {
            total_bytes: total,
            resident_bytes: 0,
        },
    };
    BTreeMap::from([
        ("krick-1".to_string(), server("krick", 26_843_545_600)),
        ("pi5v1".to_string(), server("pi5-west", 16_106_127_360)),
        ("pi5v3".to_string(), server("pi5-east", 0)),
    ])
}

/// Fragment requests for the two fixture leaves of bucket 7, bound to the
/// view's resource and topology generation.
pub fn planner_fragment_requests(view: &CommittedView) -> Vec<FragmentRequest> {
    let partition = |bucket: u64| PartitionIdentity {
        workspace: view.workspace.clone(),
        collection: view.collection.clone(),
        derived_fingerprint: view.derived_fingerprint,
        column: "key_bucket".to_string(),
        bucket,
    };
    ["L4", "L7"]
        .into_iter()
        .map(|leaf| FragmentRequest {
            partition: partition(7),
            topology_generation: view.topology_generation,
            leaf: leaf.to_string(),
        })
        .collect()
}

/// Map a real imported `ControlCollectionSnapshot` to the capacity
/// planner's public input (`TierSnapshotInput`). Authority-derived fields —
/// the authority identity, both revisions, the resource triple, the
/// topology generation, and the supplement's derived/provider digests —
/// come from the snapshot itself. Nodes, policy, fragment requests, and
/// observations are harness-built and identical across every test in this
/// suite, mirroring the in-crate planner fixtures; the import registers no
/// nodes, so a hand-built registry is expected here.
pub fn planner_input(
    snapshot: &ControlCollectionSnapshot,
    view: &CommittedView,
    observations: &ObservationStore,
) -> TierSnapshotInput {
    let authority = snapshot
        .authority
        .as_ref()
        .expect("snapshot carries the authority identity");
    let state = snapshot
        .state
        .as_ref()
        .expect("imported snapshot carries collection state");
    let supplement = state
        .configuration
        .as_ref()
        .expect("imported snapshot carries the import supplement");
    let key = snapshot
        .key
        .as_ref()
        .expect("snapshot carries the resource key");
    let provider_geometry_digest = match supplement.provider.as_ref() {
        Some(Provider::Geometry(geometry)) => sha256::digest(&geometry.encode_to_vec()),
        _ => sha256::digest(b"no-provider-geometry"),
    };
    TierSnapshotInput {
        planning_instant_unix_ms: PLANNING_INSTANT_UNIX_MS,
        max_moves: 16,
        max_observation_age_ms: COHORT_LENGTH_MS,
        clock_skew_bound_ms: 5_000,
        authority_id: sha256::to_hex(&sha256::digest(&authority.group_id)),
        authority_incarnation: authority.authority_incarnation[..]
            .try_into()
            .expect("authority incarnation is 16 bytes"),
        control_revision: snapshot.control_revision,
        policy_revision: snapshot.policy_revision,
        policy: planner_policy(),
        observation_epoch: observations.epoch(),
        topology_generation: state.topology_generation,
        placement_tree_digest: sha256::digest(b"no-placement-tree"),
        workspace: key.workspace.clone(),
        collection: key.collection.clone(),
        derived_fingerprint: view.derived_fingerprint,
        provider_geometry_digest,
        cohort_length_ms: COHORT_LENGTH_MS,
        cohort_phase_unix_ms: 0,
        nodes: planner_nodes(),
        observations: observations.observations().into_iter().cloned().collect(),
        fragment_requests: planner_fragment_requests(view),
        committed: view.clone(),
        current_incarnations: observations.current_incarnations().clone(),
    }
}
