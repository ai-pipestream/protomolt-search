//! The journal's chain, retry, paging and recovery contract, with every
//! frame admitted through a live authority session.

use super::*;
use crate::pb;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use tonic::Code;

pub(super) const PEER: [u8; 32] = [10; 32];

/// A decision admitting `PEER` under `binding`, published through a live
/// session. Admission lasts while the session is held.
pub(super) fn live<'a>(
    journal: &'a ReplayJournal,
    binding: &pb::ReplayStreamBinding,
) -> ReplayAuthoritySession<'a> {
    let policy = ReplayAdmissionPolicy {
        contract_version: 1,
        authority_id: vec![9; 16],
        revision: 1,
        binding_sha256: binding_digest(binding).unwrap(),
        source_certificate_sha256: vec![PEER.to_vec()],
        source_history_id: binding.source_history_id.clone(),
        target_history_id: binding.target_history_id.clone(),
        source_write_epoch: binding.source_write_epoch,
        target_write_epoch: binding.target_write_epoch,
        source_residency: 1,
        target_residency: 1,
        enabled: true,
    };
    let session = journal.authority_session().unwrap();
    session
        .begin_refresh(MAX_AUTHORITY_VALIDITY)
        .unwrap()
        .publish(&policy)
        .unwrap();
    session
}

/// Admit one frame from `PEER` under a session opened for the call.
pub(super) fn accept(
    journal: &ReplayJournal,
    frame: &pb::ReplayJournalFrame,
) -> Result<ReplayJournalReceipt, Status> {
    let _session = live(journal, &journal.binding);
    journal.accept_inner(frame, &PEER)
}

pub(super) struct Directory(PathBuf);
impl Directory {
    pub(super) fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "replay-journal-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    pub(super) fn path(&self) -> PathBuf {
        self.0.join("receiver.redb")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub(super) fn binding() -> pb::ReplayStreamBinding {
    pb::ReplayStreamBinding {
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
pub(super) fn record(clock: u64, text: &str) -> Vec<u8> {
    pb::wal::WalRecord {
        seq: 1,
        clock,
        op: Some(pb::wal::wal_record::Op::AddDocuments(
            pb::wal::LoggedAddDocuments {
                first_id: 400,
                documents: vec![pb::AddDocumentsRequest {
                    text: text.into(),
                    original_source: Some(pb::ProtobufSource {
                        descriptor_set: vec![10, 0],
                        message_type: "source.Doc".into(),
                        payload: vec![0x18, 0x81, 0],
                    }),
                    identity: Some(pb::DocumentIdentity {
                        document_key: vec![0, 255],
                        version: u64::MAX,
                        chunk_ordinal: None,
                    }),
                    derived_fingerprint: "f".repeat(64),
                    map_unsigned_integers: vec![pb::MapUnsignedIntegerEntry {
                        field: "values".into(),
                        key: "".into(),
                        value: u64::MAX,
                    }],
                    ..Default::default()
                }],
                stable_routing_keys: vec![vec![0, 255]],
                ..Default::default()
            },
        )),
    }
    .encode_to_vec()
}
pub(super) fn first(text: &str) -> pb::ReplayJournalFrame {
    seal_frame(
        &binding(),
        1,
        binding_digest(&binding()).unwrap(),
        record(11, text),
    )
    .unwrap()
}
pub(super) fn page(after: u64) -> pb::ReadReplayJournalRequest {
    pb::ReadReplayJournalRequest {
        after_sequence: after,
        through_sequence: None,
        limit: 1000,
        max_bytes: 1 << 20,
    }
}

#[test]
fn exact_retries_survive_reopen_and_do_not_claim_search_visibility() {
    let dir = Directory::new("retry");
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let a = first("first");
    let b = seal_frame(&binding(), 2, a.sha256.clone(), record(14, "second")).unwrap();
    let receipt = accept(&journal, &a).unwrap();
    assert!(receipt.accepted && receipt.durable && !receipt.searchable && !receipt.replayed);
    accept(&journal, &b).unwrap();
    let duplicate = accept(&journal, &a).unwrap();
    assert_eq!(
        duplicate,
        pb::ReplayJournalReceipt {
            replayed: true,
            ..receipt.clone()
        }
    );
    assert_eq!(
        accept(&journal, &first("changed")).unwrap_err().code(),
        Code::AlreadyExists
    );
    assert_eq!(journal.head().unwrap().accepted_sequence, 2);
    drop(journal);
    let journal = ReplayJournal::open(&dir.path(), binding()).unwrap();
    assert_eq!(accept(&journal, &a).unwrap(), duplicate);
    let read = journal.read(&page(0)).unwrap();
    assert_eq!(read.frames, vec![a, b]);
    assert!(read.complete);
    // The journal retained full original bytes, exact maps and source identity.
    assert_eq!(read.frames[0].record, record(11, "first"));
}

#[test]
fn gaps_divergence_repeated_clocks_and_forged_digests_fail_before_acceptance() {
    let dir = Directory::new("order");
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let a = first("first");
    accept(&journal, &a).unwrap();
    let cases = [
        seal_frame(&binding(), 3, a.sha256.clone(), record(12, "gap")).unwrap(),
        seal_frame(&binding(), 2, vec![99; 32], record(12, "fork")).unwrap(),
        seal_frame(&binding(), 2, a.sha256.clone(), record(11, "repeat clock")).unwrap(),
        seal_frame(&binding(), 2, a.sha256.clone(), record(9, "old clock")).unwrap(),
    ];
    for bad in cases {
        assert_eq!(
            accept(&journal, &bad).unwrap_err().code(),
            Code::FailedPrecondition
        );
    }
    let mut forged = a.clone();
    forged.record = record(11, "tampered");
    assert_eq!(
        accept(&journal, &forged).unwrap_err().code(),
        Code::InvalidArgument
    );
    assert_eq!(journal.head().unwrap().accepted_sequence, 1);
    let malformed = vec![0x08, 0x80];
    assert!(seal_frame(&binding(), 2, a.sha256.clone(), malformed).is_err());
    assert!(seal_frame(&binding(), 0, a.sha256.clone(), record(12, "zero")).is_err());
    drop(journal);
    assert_eq!(
        ReplayJournal::open(&dir.path(), binding())
            .unwrap()
            .read(&page(0))
            .unwrap()
            .frames,
        [a]
    );
}

#[test]
fn every_assignment_component_is_bound_and_reassignment_cannot_reuse_the_file() {
    let dir = Directory::new("binding");
    let base = binding();
    let journal = ReplayJournal::create(&dir.path(), base.clone()).unwrap();
    accept(&journal, &first("doc")).unwrap();
    assert!(
        ReplayJournal::open(&dir.path(), base.clone()).is_err(),
        "exclusive lifetime lock"
    );
    assert!(
        ReplayJournal::create(&dir.path(), base.clone()).is_err(),
        "create never truncates"
    );
    drop(journal);
    for i in 0..14 {
        let mut changed = base.clone();
        match i {
            0 => changed.workspace = "other".into(),
            1 => changed.collection = "other".into(),
            2 => changed.source_shard_id = "other".into(),
            3 => changed.source_history_id[0] ^= 1,
            4 => changed.source_wal_generation += 1,
            5 => changed.source_write_epoch += 1,
            6 => changed.target_shard_id = "other".into(),
            7 => changed.target_history_id[0] ^= 1,
            8 => changed.target_write_epoch += 1,
            9 => changed.topology_revision += 1,
            10 => changed.assignment_id[0] ^= 1,
            11 => changed.baseline_clock += 1,
            12 => changed.baseline_sha256[0] ^= 1,
            13 => changed.index_contract_sha256[0] ^= 1,
            _ => unreachable!(),
        }
        assert_ne!(
            binding_digest(&base).unwrap(),
            binding_digest(&changed).unwrap()
        );
        assert_eq!(
            ReplayJournal::open(&dir.path(), changed)
                .err()
                .unwrap()
                .code(),
            Code::FailedPrecondition
        );
    }
    assert_eq!(
        ReplayJournal::open(&dir.path(), base)
            .unwrap()
            .head()
            .unwrap()
            .accepted_sequence,
        1
    );
    let mut invalid = binding();
    invalid.source_write_epoch = 0;
    assert!(binding_digest(&invalid).is_err());
    invalid = binding();
    invalid.assignment_id.clear();
    assert!(binding_digest(&invalid).is_err());
}

#[test]
fn pages_are_byte_bounded_and_pin_their_prefix_across_later_appends() {
    let dir = Directory::new("pages");
    let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
    let a = first("a");
    let b = seal_frame(
        &binding(),
        2,
        a.sha256.clone(),
        record(12, &"large".repeat(1000)),
    )
    .unwrap();
    let c = seal_frame(&binding(), 3, b.sha256.clone(), record(13, "c")).unwrap();
    for frame in [&a, &b] {
        accept(&journal, frame).unwrap();
    }
    let mut request = page(0);
    request.max_bytes = a.encoded_len() as u64;
    let initial = journal.read(&request).unwrap();
    assert_eq!(initial.frames, std::slice::from_ref(&a));
    assert!(!initial.complete);
    accept(&journal, &c).unwrap();
    request.after_sequence = initial.next_sequence;
    request.through_sequence = Some(initial.through_sequence);
    assert_eq!(
        journal.read(&request).unwrap_err().code(),
        Code::ResourceExhausted
    );
    request.max_bytes = b.encoded_len() as u64;
    let rest = journal.read(&request).unwrap();
    assert_eq!(rest.frames, [b]);
    assert!(rest.complete);
    assert_eq!(rest.through_sequence, 2);
    assert_eq!(journal.read(&page(2)).unwrap().frames, [c]);
    request = page(0);
    request.through_sequence = Some(0);
    assert!(journal.read(&request).unwrap().frames.is_empty());
    request = page(4);
    assert!(journal.read(&request).is_err());
    request = page(0);
    request.limit = 0;
    assert!(journal.read(&request).is_err());
    request = page(0);
    request.max_bytes = 0;
    assert!(journal.read(&request).is_err());
}

#[test]
fn concurrent_duplicate_deliveries_share_one_durable_decision() {
    let dir = Directory::new("concurrent");
    let journal = Arc::new(ReplayJournal::create(&dir.path(), binding()).unwrap());
    let _session = live(&journal, &binding());
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let journal = journal.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                journal.accept_inner(&first("same"), &PEER).unwrap()
            })
        })
        .collect();
    let receipts: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(receipts.iter().filter(|r| !r.replayed).count(), 1);
    assert!(receipts.iter().all(|r| r.durable && !r.searchable));
    assert_eq!(journal.head().unwrap().accepted_sequence, 1);
}
