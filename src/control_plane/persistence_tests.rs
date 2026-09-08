use super::*;
use std::time::Duration;

struct Directory(PathBuf);
impl Directory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "control-persistence-{name}-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn state(&self) -> PathBuf {
        self.0.join("state.json")
    }

    fn stored(&self) -> StoredState {
        serde_json::from_slice(&std::fs::read(self.state()).unwrap()).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn registration(id: &str) -> RegisterNodeRequest {
    RegisterNodeRequest {
        collection: String::new(),
        node_id: id.into(),
        addr: format!("http://{id}"),
        capacity: Some(NodeCapacity {
            disk_bytes: 1_000,
            failure_domain: "az-a".into(),
            ..Default::default()
        }),
        lease_ms: 10_000,
    }
}

fn arm(plane: &DurableControlPlane, fault: StateWriteFault) {
    *plane.write_fault.lock().unwrap() = Some(fault);
}

fn candidate_files(dir: &Directory) -> Vec<PathBuf> {
    let mut files: Vec<_> = std::fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("state.json.tmp-")
        })
        .collect();
    files.sort();
    files
}

#[test]
fn failure_before_rename_rolls_back_disk_and_memory_and_remains_usable() {
    let dir = Directory::new("before-rename");
    let plane = DurableControlPlane::open(dir.state(), ControlPolicy::default()).unwrap();
    let before = plane.plan().unwrap();
    assert_eq!(before.control_revision, 1);
    let candidates_before = candidate_files(&dir);

    arm(&plane, StateWriteFault::BeforeRename);
    let error = plane.register(registration("a"), 100).unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(error.message().contains("BeforeRename"), "{error}");
    let memory = plane.plan().unwrap();
    let disk = dir.stored();
    assert_eq!(memory.control_revision, before.control_revision);
    assert!(memory.nodes.is_empty());
    assert_eq!(disk.revision, before.control_revision);
    assert!(disk.nodes.is_empty());
    assert_eq!(candidate_files(&dir), candidates_before);

    let accepted = plane.register(registration("b"), 100).unwrap();
    assert_eq!(accepted.control_revision, 2);
    assert_eq!(plane.plan().unwrap().nodes.len(), 1);
    assert_eq!(dir.stored().nodes.len(), 1);
}

#[test]
fn failure_after_rename_latches_every_clone_until_an_explicit_reopen() {
    let dir = Directory::new("after-rename");
    let plane = DurableControlPlane::open(dir.state(), ControlPolicy::default()).unwrap();
    let clone = plane.clone();
    let cross_thread = plane.clone();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let checking = std::thread::spawn(move || {
        start_rx.recv().unwrap();
        result_tx.send(cross_thread.plan()).unwrap();
    });

    arm(&plane, StateWriteFault::AfterRename);
    let initial_error = plane.register(registration("a"), 100).unwrap_err();
    let disk = dir.stored();

    start_tx.send(()).unwrap();
    let cross_thread_result = result_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("cross-thread clone read remained blocked after the persistence failure");
    checking.join().unwrap();
    let original_read = plane.plan().err().expect("original must be latched");
    let clone_topology = clone
        .topology_routes()
        .err()
        .expect("clone topology read must be latched");
    let clone_mutation = clone
        .register(registration("b"), 100)
        .err()
        .expect("clone mutation must be latched");
    let service = ClusterControlService::new(clone.clone());
    let service_admission = service
        .admit("")
        .err()
        .expect("service admission must observe the latch");
    let service_publication = service
        .publish_current_topology()
        .err()
        .expect("topology publication must observe the latch without a coordinator");

    assert_eq!(disk.revision, 2);
    assert!(disk.nodes.contains_key("a"));
    for error in [
        original_read,
        clone_topology,
        cross_thread_result
            .err()
            .expect("cross-thread clone must be latched"),
        clone_mutation,
        service_admission,
        service_publication,
    ] {
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("persistence outcome is uncertain"),
            "{error}"
        );
    }
    assert_eq!(initial_error.code(), tonic::Code::Internal);
    assert!(
        initial_error
            .message()
            .contains("persistence outcome is uncertain"),
        "{initial_error}"
    );

    let latched = clone;
    drop(plane);
    let locked = DurableControlPlane::open_existing(dir.state(), ControlPolicy::default())
        .err()
        .expect("a live failed clone must retain exclusive ownership");
    assert!(
        locked.contains("exclusive") && locked.contains("lock"),
        "{locked}"
    );
    let still_latched = latched
        .plan()
        .err()
        .expect("old clone must remain latched before recovery");
    assert_eq!(still_latched.code(), tonic::Code::FailedPrecondition);
    drop(service);
    drop(latched);
    let reopened =
        DurableControlPlane::open_existing(dir.state(), ControlPolicy::default()).unwrap();
    let recovered = reopened.plan().unwrap();
    assert_eq!(recovered.control_revision, 2);
    assert_eq!(recovered.nodes.len(), 1);
    assert_eq!(recovered.nodes[0].node_id, "a");
    let next = reopened.register(registration("b"), 100).unwrap();
    assert_eq!(next.control_revision, 3);
}

#[test]
fn collection_binding_before_rename_failure_is_transactional() {
    let dir = Directory::new("collection-before-rename");
    let plane = DurableControlPlane::open(dir.state(), ControlPolicy::default()).unwrap();
    let observer = plane.clone();
    arm(&plane, StateWriteFault::BeforeRename);

    let error = plane.clone().with_collection("books").err().unwrap();
    assert!(error.contains("BeforeRename"), "{error}");
    assert_eq!(observer.collection(), "");
    assert_eq!(dir.stored().collection, "");

    let bound = observer.with_collection("books").unwrap();
    assert_eq!(bound.collection(), "books");
    assert_eq!(dir.stored().collection, "books");
    assert_eq!(
        bound
            .register(registration("a"), 100)
            .unwrap()
            .control_revision,
        2
    );
}

#[test]
fn collection_binding_after_rename_requires_an_explicit_reopen() {
    let dir = Directory::new("collection-after-rename");
    let plane = DurableControlPlane::open(dir.state(), ControlPolicy::default()).unwrap();
    let observer = plane.clone();
    arm(&plane, StateWriteFault::AfterRename);

    let initial_error = plane.clone().with_collection("books").err().unwrap();
    let disk_collection = dir.stored().collection;
    let refused = observer
        .plan()
        .err()
        .expect("shared instance must be latched");

    assert_eq!(disk_collection, "books");
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(refused
        .message()
        .contains("persistence outcome is uncertain"));
    assert!(
        initial_error.contains("persistence outcome is uncertain"),
        "{initial_error}"
    );

    drop(plane);
    let locked = DurableControlPlane::open_existing(dir.state(), ControlPolicy::default())
        .err()
        .expect("observer clone must retain exclusive ownership");
    assert!(
        locked.contains("exclusive") && locked.contains("lock"),
        "{locked}"
    );
    assert_eq!(
        observer.plan().err().unwrap().code(),
        tonic::Code::FailedPrecondition
    );
    drop(observer);
    let reopened = DurableControlPlane::open_existing(dir.state(), ControlPolicy::default())
        .unwrap()
        .with_collection("books")
        .unwrap();
    assert_eq!(reopened.collection(), "books");
    assert_eq!(reopened.plan().unwrap().control_revision, 1);
}

#[test]
fn reconcile_topology_overflow_leaves_serialized_memory_and_disk_unchanged() {
    let dir = Directory::new("reconcile-overflow");
    let plane = DurableControlPlane::open(
        dir.state(),
        ControlPolicy {
            replication_factor: 1,
            split_rows: u64::MAX,
            merge_rows: 0,
            compact_segments: u32::MAX,
            compact_tombstone_ppm: u32::MAX,
            ..Default::default()
        },
    )
    .unwrap();
    let lease = plane.register(registration("a"), 100).unwrap();
    plane
        .report(
            ReportShardRequest {
                collection: String::new(),
                node_id: lease.node_id.clone(),
                lease_token: lease.lease_token,
                replica: Some(ShardReplicaState {
                    shard_id: "s0".into(),
                    generation: 7,
                    hash_lo: 0,
                    hash_hi: u64::MAX,
                    rows: 10,
                    role: ShardReplicaRole::Primary as i32,
                    ready: true,
                    scoring_fingerprint: "score-v1".into(),
                    analysis_fingerprint: "analysis-v1".into(),
                    immutable_segments: 1,
                    ..Default::default()
                }),
            },
            100,
        )
        .unwrap();
    let memory_before = {
        let mut state = plane.state.lock().unwrap();
        state.topology = StoredTopology {
            generation: u64::MAX,
            routes: Vec::new(),
        };
        plane.persist_locked(&state).unwrap();
        serde_json::to_vec(&*state).unwrap()
    };
    let disk_before = std::fs::read(dir.state()).unwrap();

    let error = plane.reconcile(false, 100).err().unwrap();
    let memory_after = {
        let state = plane.state.lock().unwrap();
        serde_json::to_vec(&*state).unwrap()
    };
    let disk_after = std::fs::read(dir.state()).unwrap();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("topology generation overflow"),
        "{error}"
    );
    assert_eq!(memory_after, memory_before);
    assert_eq!(disk_after, disk_before);
}

#[test]
fn open_existing_refuses_missing_corrupt_and_wrong_format_without_side_effects() {
    let dir = Directory::new("open-existing-refusals");
    let missing = dir.0.join("missing.json");
    let missing_error = DurableControlPlane::open_existing(&missing, ControlPolicy::default())
        .err()
        .expect("missing authority must not be bootstrapped during recovery");
    assert!(missing_error.contains("read"), "{missing_error}");
    assert!(!missing.exists());

    let corrupt = dir.0.join("corrupt.json");
    let corrupt_bytes = b"{ private invalid control state".to_vec();
    std::fs::write(&corrupt, &corrupt_bytes).unwrap();
    let corrupt_error = DurableControlPlane::open_existing(&corrupt, ControlPolicy::default())
        .err()
        .expect("corrupt authority must be refused");
    assert!(corrupt_error.contains("parse"), "{corrupt_error}");
    assert_eq!(std::fs::read(&corrupt).unwrap(), corrupt_bytes);

    let wrong_format = dir.0.join("wrong-format.json");
    let mut invalid = StoredState::default();
    invalid.format = 99;
    let wrong_format_bytes = serde_json::to_vec_pretty(&invalid).unwrap();
    std::fs::write(&wrong_format, &wrong_format_bytes).unwrap();
    let format_error = DurableControlPlane::open_existing(&wrong_format, ControlPolicy::default())
        .err()
        .expect("unknown authority format must be refused");
    assert!(format_error.contains("expected 1"), "{format_error}");
    assert_eq!(std::fs::read(&wrong_format).unwrap(), wrong_format_bytes);
}
