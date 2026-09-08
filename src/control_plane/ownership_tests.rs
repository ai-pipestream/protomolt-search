use super::*;

struct Directory(PathBuf);

impl Directory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "control-ownership-{name}-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn state(&self) -> PathBuf {
        self.0.join("state.json")
    }

    fn lock(&self) -> PathBuf {
        self.0.join("state.json.lock")
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

fn assert_lock_refusal(result: Result<DurableControlPlane, String>) {
    let error = result
        .err()
        .expect("a second control owner must be refused");
    assert!(error.contains("exclusive"), "{error}");
    assert!(error.contains("lock"), "{error}");
}

#[test]
fn independent_opens_are_refused_until_the_last_clone_releases_ownership() {
    let dir = Directory::new("clones");
    let plane = DurableControlPlane::open(dir.state(), ControlPolicy::default()).unwrap();
    let clone = plane.clone();
    assert_eq!(
        plane
            .register(registration("winner"), 100)
            .unwrap()
            .control_revision,
        2
    );

    assert_lock_refusal(DurableControlPlane::open(
        dir.state(),
        ControlPolicy::default(),
    ));
    assert_lock_refusal(DurableControlPlane::open_existing(
        dir.state(),
        ControlPolicy::default(),
    ));
    let winner = plane.plan().unwrap();
    assert_eq!(winner.control_revision, 2);
    assert_eq!(winner.nodes.len(), 1);
    assert_eq!(winner.nodes[0].node_id, "winner");

    drop(plane);
    assert_lock_refusal(DurableControlPlane::open_existing(
        dir.state(),
        ControlPolicy::default(),
    ));
    assert_eq!(clone.plan().unwrap(), winner);
    drop(clone);

    let reopened =
        DurableControlPlane::open_existing(dir.state(), ControlPolicy::default()).unwrap();
    assert_eq!(reopened.plan().unwrap(), winner);
}

#[test]
fn independent_open_worker() {
    let Some(path) = std::env::var_os("PSEARCH_CONTROL_OWNERSHIP_PATH") else {
        return;
    };
    match DurableControlPlane::open_existing(PathBuf::from(path), ControlPolicy::default()) {
        Err(error) if error.contains("exclusive") && error.contains("lock") => {
            std::process::exit(73)
        }
        Err(error) => panic!("independent open returned the wrong error: {error}"),
        Ok(_) => std::process::exit(74),
    }
}

#[test]
fn another_process_cannot_open_the_live_control_store() {
    let dir = Directory::new("process");
    let plane = DurableControlPlane::open(dir.state(), ControlPolicy::default()).unwrap();
    assert_eq!(
        plane
            .register(registration("winner"), 100)
            .unwrap()
            .control_revision,
        2
    );

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("control_plane::ownership_tests::independent_open_worker")
        .arg("--nocapture")
        .env("PSEARCH_CONTROL_OWNERSHIP_PATH", dir.state())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let winner = plane.plan().unwrap();
    assert_eq!(winner.control_revision, 2);
    assert_eq!(winner.nodes.len(), 1);
    assert_eq!(winner.nodes[0].node_id, "winner");
}

#[test]
fn repeated_state_renames_keep_the_same_exclusive_owner() {
    let dir = Directory::new("renames");
    let plane = DurableControlPlane::open(dir.state(), ControlPolicy::default()).unwrap();
    for (revision, node) in [(2, "a"), (3, "b"), (4, "c")] {
        assert_eq!(
            plane
                .register(registration(node), 100)
                .unwrap()
                .control_revision,
            revision
        );
        assert_lock_refusal(DurableControlPlane::open_existing(
            dir.state(),
            ControlPolicy::default(),
        ));
    }
    assert_eq!(plane.plan().unwrap().nodes.len(), 3);
}

#[cfg(unix)]
#[test]
fn a_parent_symlink_alias_cannot_bypass_the_live_lock() {
    use std::os::unix::fs::symlink;

    let dir = Directory::new("parent-alias");
    let plane = DurableControlPlane::open(dir.state(), ControlPolicy::default()).unwrap();
    let alias = dir.0.join("alias");
    symlink(&dir.0, &alias).unwrap();
    assert_lock_refusal(DurableControlPlane::open_existing(
        alias.join("state.json"),
        ControlPolicy::default(),
    ));
    assert_eq!(plane.plan().unwrap().control_revision, 1);
}

#[cfg(unix)]
#[test]
fn symlink_state_and_lock_leaves_are_refused_without_following_targets() {
    use std::os::unix::fs::symlink;

    for leaf in ["state", "lock"] {
        let dir = Directory::new(&format!("symlink-{leaf}"));
        let external = dir.0.join("external");
        let sentinel = b"external target must remain unchanged";
        std::fs::write(&external, sentinel).unwrap();
        if leaf == "state" {
            symlink(&external, dir.state()).unwrap();
        } else {
            symlink(&external, dir.lock()).unwrap();
        }
        let error = DurableControlPlane::open(dir.state(), ControlPolicy::default())
            .err()
            .expect("symlink leaf must be refused");
        assert!(error.contains("symlink"), "{leaf}: {error}");
        assert_eq!(std::fs::read(&external).unwrap(), sentinel, "{leaf}");
    }
}

#[cfg(unix)]
#[test]
fn durable_state_and_lock_files_are_private() {
    use std::os::unix::fs::PermissionsExt;

    let dir = Directory::new("private-files");
    let plane = DurableControlPlane::open(dir.state(), ControlPolicy::default()).unwrap();
    assert_eq!(
        plane
            .register(registration("a"), 100)
            .unwrap()
            .control_revision,
        2
    );
    assert_eq!(
        std::fs::metadata(dir.state()).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(dir.lock()).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn abrupt_holder_worker() {
    let Some(path) = std::env::var_os("PSEARCH_CONTROL_ABRUPT_HOLDER_PATH") else {
        return;
    };
    let plane = DurableControlPlane::open(PathBuf::from(path), ControlPolicy::default()).unwrap();
    assert_eq!(
        plane
            .register(registration("winner"), 100)
            .unwrap()
            .control_revision,
        2
    );
    std::process::exit(87);
}

#[test]
fn abrupt_holder_exit_releases_lock_and_preserves_committed_state() {
    let dir = Directory::new("abrupt-holder");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("control_plane::ownership_tests::abrupt_holder_worker")
        .arg("--nocapture")
        .env("PSEARCH_CONTROL_ABRUPT_HOLDER_PATH", dir.state())
        .env_remove("PSEARCH_CONTROL_OWNERSHIP_PATH")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(87));

    let reopened =
        DurableControlPlane::open_existing(dir.state(), ControlPolicy::default()).unwrap();
    let plan = reopened.plan().unwrap();
    assert_eq!(plan.control_revision, 2);
    assert_eq!(plan.nodes.len(), 1);
    assert_eq!(plan.nodes[0].node_id, "winner");
}

#[cfg(unix)]
#[test]
fn private_candidates_do_not_clobber_or_outlive_their_owner() {
    use std::os::unix::fs::PermissionsExt;

    let dir = Directory::new("candidate");
    let sentinel = dir.0.join(format!("state.json.tmp-{}", std::process::id()));
    let sentinel_bytes = b"pre-existing candidate belongs to another writer";
    std::fs::write(&sentinel, sentinel_bytes).unwrap();

    let candidate = storage::Candidate::write(&dir.state(), b"candidate-state").unwrap();
    let mut candidates: Vec<_> = std::fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("state.json.tmp-")
                && path != &sentinel
        })
        .collect();
    assert_eq!(candidates.len(), 1);
    let owned = candidates.pop().unwrap();
    assert_eq!(std::fs::read(&owned).unwrap(), b"candidate-state");
    assert_eq!(
        std::fs::metadata(&owned).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(std::fs::read(&sentinel).unwrap(), sentinel_bytes);

    drop(candidate);
    assert!(!owned.exists());
    assert_eq!(std::fs::read(&sentinel).unwrap(), sentinel_bytes);
}
