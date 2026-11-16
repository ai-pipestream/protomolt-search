//! Who may append to a journal: the persisted decision, the live session
//! behind it, and the verified transport peer.

use super::*;

mod policy;
mod session;
#[cfg(feature = "tls")]
mod transport;

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "replay-admission-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.join("journal.redb")
    }
}
impl Drop for Directory {
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
        source_write_epoch: 2,
        target_shard_id: "target".into(),
        target_history_id: vec![3; 16],
        target_write_epoch: 4,
        topology_revision: 5,
        assignment_id: vec![6; 16],
        baseline_sha256: vec![7; 32],
        index_contract_sha256: vec![8; 32],
        ..Default::default()
    }
}
fn policy(revision: u64) -> ReplayAdmissionPolicy {
    let b = binding();
    ReplayAdmissionPolicy {
        contract_version: 1,
        authority_id: vec![9; 16],
        revision,
        binding_sha256: binding_digest(&b).unwrap(),
        source_certificate_sha256: vec![vec![10; 32]],
        source_history_id: b.source_history_id,
        target_history_id: b.target_history_id,
        source_write_epoch: b.source_write_epoch,
        target_write_epoch: b.target_write_epoch,
        source_residency: 1,
        target_residency: 1,
        enabled: true,
    }
}
fn frame() -> ReplayJournalFrame {
    seal_frame(
        &binding(),
        1,
        binding_digest(&binding()).unwrap(),
        vec![8, 1, 0x30, 1, 0x22, 0],
    )
    .unwrap()
}

fn grant_live<'a>(
    journal: &'a ReplayJournal,
    policy: &ReplayAdmissionPolicy,
) -> ReplayAuthoritySession<'a> {
    let session = journal.authority_session().unwrap();
    session
        .begin_refresh(MAX_AUTHORITY_VALIDITY)
        .unwrap()
        .publish(policy)
        .unwrap();
    session
}
