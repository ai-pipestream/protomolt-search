//! Shared kit for the control-authority adversarial suite
//! (`docs/control-authority-test-harness.md`).
//!
//! Everything here mirrors the in-crate `src/source_authority/control_tests.rs`
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
use pipestream_search::pb::storage::capacity_transition::Action as CapacityAction;
use pipestream_search::pb::storage::control_configuration::{Derived, Placement, Provider};
use pipestream_search::pb::storage::source_authority_command::Action;
use pipestream_search::pb::storage::{
    ActivateSourceOwner, CancelPreparedSourceOwner, CapacityConfiguration,
    CapacityConfigureCommand, CapacityObservation, CapacityPartition, CapacityResidency,
    CapacityShardRef, CapacityTierPolicy, CapacityTierSpec, CapacityTransition,
    CommitCapacityExpiry, ConfirmSourceOwnerReady, ControlConfiguration, ControlInstallCommand,
    ControlInstallReceipt, ControlInstallState, ControlNodeRecord, ControlPlannerPolicy,
    ControlProviderGeometry, ControlReconcilePolicy, ControlReplicaRecord, ControlRoute,
    ControlRouteTable, LogicalSourceOwner, PlacementRouteCode, RegisterCapacityReporter,
    ReplaceSourceCollectionGrants, SourceAuthorityCommand, SourceAuthorityIdentity,
    SourceAuthorityLimits, SourceOwnerCompletion, SourceResidency, SourceStorageTarget,
};
use pipestream_search::pb::{
    AccessAction, AccessPolicy, ClusterNodeState, CollectionGrant, CollectionResource,
    NodeCapacity, NodeResidency, PlacementNode, PlacementTree, ShardReplicaRole,
};
use pipestream_search::source_authority::{PlanningContext, SourceAuthorityStore};
use pipestream_search::test_support::ForkGuarded;

pub const WORKER_ENV: &str = "PSEARCH_ADV_WORKER";
pub const WORKER_DIR_ENV: &str = "PSEARCH_ADV_DIR";
pub const WORKER_ARM_ENV: &str = "PSEARCH_ADV_ARM";
pub const WORKSPACE: &str = "test";
pub const COLLECTION: &str = "books";
pub const WORKFLOW: &[u8] = b"workflow-1";
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

/// The collection resource key (empty owner), used by policy commands and installs.
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

/// The minimal valid configuration: no placement tree, no derived columns
/// (mirrors `control_tests.rs`).
pub fn configuration() -> ControlConfiguration {
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

/// The inclusive hash range of route `index` of `count` routes tiling the
/// u64 hash space in order.
pub fn route_range(index: usize, count: usize) -> (u64, u64) {
    let space = u64::MAX as u128 + 1;
    (
        (index as u128 * space / count as u128) as u64,
        ((index as u128 + 1) * space / count as u128 - 1) as u64,
    )
}

/// `count` routes on 127.0.0.1:9000.. tiling the hash space.
pub fn routes(count: usize) -> Vec<ControlRoute> {
    (0..count)
        .map(|index| {
            let (hash_lo, hash_hi) = route_range(index, count);
            ControlRoute {
                addr: format!("127.0.0.1:{}", 9_000 + index),
                replica: None,
                hash_lo,
                hash_hi,
            }
        })
        .collect()
}

/// A bare collection: `routes` routes at generation 1, no nodes, replicas
/// or actions.
pub fn install_state(routes: usize) -> ControlInstallState {
    ControlInstallState {
        next_token: 1,
        next_action: 1,
        topology: Some(ControlRouteTable {
            generation: 1,
            routes: self::routes(routes),
        }),
        history: Vec::new(),
        nodes: Vec::new(),
        replicas: Vec::new(),
        actions: Vec::new(),
        completed_actions: Vec::new(),
        configuration: Some(configuration()),
    }
}

pub fn install_command(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control_revision: u64,
    state: ControlInstallState,
) -> ControlInstallCommand {
    install_command_policy(authority, id, control_revision, 1, state)
}

/// The install builder with an explicit expected policy revision (the
/// fixed-suite [`install_command`] pins 1, the bootstrap revision).
pub fn install_command_policy(
    authority: &SourceAuthorityIdentity,
    id: &str,
    control_revision: u64,
    policy_revision: u64,
    state: ControlInstallState,
) -> ControlInstallCommand {
    ControlInstallCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(key()),
        command_id: id.as_bytes().to_vec(),
        expected_control_revision: control_revision,
        expected_policy_revision: policy_revision,
        state: Some(state),
    }
}

/// Install `state` at control revision 1; returns the receipt (control
/// revision 2).
pub fn run_install(
    store: &SourceAuthorityStore,
    authority: &SourceAuthorityIdentity,
    state: ControlInstallState,
) -> ControlInstallReceipt {
    let decision = store
        .install_control("alice", &install_command(authority, "install", 1, state))
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    decision.receipt.unwrap()
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

// ---- general command builders ----------------------------------------------

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
/// The spawn is guarded (`ForkGuarded`): a lock a sibling test drops while
/// the child is between fork and exec is not held by the child.
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
    command.spawn_guarded().expect("spawn adversarial worker")
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
        .status_guarded()
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

/// How long an armed worker waits for the parent's `go` file at each
/// durable step before exiting cleanly (which fails the parent's kill
/// assertion loudly).
pub const WORKER_GO_TIMEOUT: Duration = Duration::from_secs(120);

// ---- capacity-planner leg (slice 3b: the persisted adapter) -----------------

/// Cohort length shared by every capacity configuration in this suite; the
/// observation windows are `[PLANNING_INSTANT_UNIX_MS - CAPACITY_COHORT_MS,
/// PLANNING_INSTANT_UNIX_MS)`, aligned to phase 0.
pub const CAPACITY_COHORT_MS: u64 = 600_000;
/// Fixed planning instant: twenty cohorts past the epoch at phase 0. Node
/// eligibility compares the committed lease expiry against this instant; the
/// installed leases expire far later, so the instant stays deterministic.
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

/// The kit configuration plus the placement tree and one route code per
/// route: route 0 names leaf "old", route 1 names leaf "rest". The planner
/// adapter names a shard's leaf only when the install carried placement for
/// the route whose hash range exactly matches the shard's primary.
pub fn placement_configuration() -> ControlConfiguration {
    let mut configuration = configuration();
    configuration.placement = Some(Placement::Tree(placement_tree()));
    configuration.route_codes = ["old", "rest"]
        .map(|name| PlacementRouteCode {
            has_placement: true,
            placement: leaf_code(name),
        })
        .to_vec();
    configuration
}

/// A 2-route collection with two registered server nodes (rack-a, rack-b)
/// and one ready primary per route — shard-a on node-a in route 0, shard-b
/// on node-b in route 1 — exactly the committed shape the capacity adapter
/// derives shards, leaves, pools and eligible nodes from. Leases expire far
/// past the fixed planning instant.
pub fn cluster_state() -> ControlInstallState {
    let node = |node_id: &str, port: u16, domain: &str, lease_token: u64| ControlNodeRecord {
        node_id: node_id.into(),
        addr: format!("127.0.0.1:{port}"),
        state: ClusterNodeState::Active as i32,
        lease_token,
        expires_unix_ms: 1 << 50,
        capacity: Some(NodeCapacity {
            disk_bytes: 26_843_545_600,
            used_disk_bytes: 0,
            failure_domain: domain.into(),
            residency: NodeResidency::Server as i32,
            ..Default::default()
        }),
    };
    let replica = |node: &ControlNodeRecord, shard: &str, route: usize| {
        let (hash_lo, hash_hi) = route_range(route, 2);
        ControlReplicaRecord {
            shard_id: shard.into(),
            node_id: node.node_id.clone(),
            addr: node.addr.clone(),
            generation: SOURCE_GENERATION,
            hash_lo,
            hash_hi,
            rows: 1_000_000,
            bytes: 268_435_456,
            role: ShardReplicaRole::Primary as i32,
            ready: true,
            ..Default::default()
        }
    };
    let node_a = node(NODE_A, 9_101, "rack-a", 1);
    let node_b = node(NODE_B, 9_102, "rack-b", 2);
    ControlInstallState {
        next_token: 3,
        replicas: vec![replica(&node_a, SHARD_A, 0), replica(&node_b, SHARD_B, 1)],
        nodes: vec![node_a, node_b],
        configuration: Some(placement_configuration()),
        ..install_state(2)
    }
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

#[allow(clippy::too_many_arguments)]
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

/// The topology generation the committed control state carries, read back
/// from the snapshot rather than assumed.
pub fn committed_generation(store: &SourceAuthorityStore) -> u64 {
    let snapshot = store.control_snapshot("alice", &key()).unwrap();
    snapshot
        .state
        .as_ref()
        .expect("installed snapshot carries collection state")
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

/// A store that went through the full adapter flow: the cluster state
/// installed, capacity configured at control revision 2, both reporters
/// registered, and one report per shard in the current cohort window.
/// Observation epoch is 2.
pub struct AdapterFixture {
    pub dir: TestDir,
    pub authority: SourceAuthorityIdentity,
    pub store: SourceAuthorityStore,
    /// The committed topology generation the reports and context bind to.
    pub generation: u64,
}

pub fn adapter_fixture(name: &str) -> AdapterFixture {
    let dir = TestDir::new(name);
    let (store, authority) = create_store(&dir);
    run_install(&store, &authority, cluster_state());
    let generation = committed_generation(&store);
    // The install commits at control revision 2; configure is next.
    let decision = store
        .configure_capacity(
            "alice",
            &configure_command(
                &authority,
                "cfg",
                2,
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
