//! One access policy format and one replay journal format: a retired
//! version is refused, and a new journal starts under authority.

use std::path::PathBuf;

use pipestream_search::authorization::PolicyAuthority;
use pipestream_search::pb::{
    AccessAction, AccessPolicy, CollectionGrant, CollectionResource, ReplayStreamBinding,
};
use pipestream_search::replay_journal::ReplayJournal;

fn policy(format_version: u32) -> AccessPolicy {
    AccessPolicy {
        format_version,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "workspace".into(),
            collection: "books".into(),
        }],
        grants: vec![CollectionGrant {
            principal: "reader".into(),
            workspace: "workspace".into(),
            collection: "books".into(),
            actions: vec![AccessAction::Search as i32],
            document_visibility: None,
            field_permissions: None,
        }],
    }
}

#[test]
fn only_the_current_access_policy_format_is_accepted() {
    PolicyAuthority::new(policy(3)).expect("the current format is accepted");
    for retired in [1, 2] {
        let err = match PolicyAuthority::new(policy(retired)) {
            Ok(_) => panic!("an access policy at retired format {retired} was accepted"),
            Err(err) => err,
        };
        assert!(
            err.contains(&format!("format_version {retired}")) && err.contains("expected 3"),
            "{err}"
        );
    }
}

/// A scratch directory removed when the test ends, pass or fail.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("wave2-ladders-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn binding() -> ReplayStreamBinding {
    ReplayStreamBinding {
        contract_version: 1,
        workspace: "work".into(),
        collection: "docs".into(),
        source_shard_id: "source".into(),
        source_history_id: vec![1; 16],
        source_wal_generation: 0,
        source_write_epoch: 7,
        target_shard_id: "replica".into(),
        target_history_id: vec![2; 16],
        target_write_epoch: 9,
        topology_revision: 12,
        assignment_id: vec![3; 16],
        baseline_clock: 10,
        baseline_sha256: vec![4; 32],
        index_contract_sha256: vec![5; 32],
    }
}

#[test]
fn a_new_replay_journal_starts_at_the_one_authorized_format() {
    let dir = TempDir::new("journal");
    let path = dir.0.join("receiver.redb");
    let journal = ReplayJournal::create(&path, binding()).unwrap();
    let head = journal.head().unwrap();
    assert_eq!(
        head.format_version, 3,
        "a new journal was created in the unauthenticated local format"
    );
    assert_eq!(head.accepted_sequence, 0);
    drop(journal);
    // The format survives a reopen with no authority decision installed.
    let reopened = ReplayJournal::open(&path, binding()).unwrap();
    assert_eq!(reopened.head().unwrap().format_version, 3);
}
