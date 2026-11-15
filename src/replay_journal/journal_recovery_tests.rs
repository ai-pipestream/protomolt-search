//! The journal across process exit, lost or damaged history, and unusual
//! but legal record encodings.

use super::journal_tests::{accept, binding, first, page, record, Directory};
use super::*;
use crate::pb;
use crate::test_support::ForkGuarded;
use tonic::Code;

#[test]
fn accepted_receipt_survives_process_exit_without_drop() {
    const ENV: &str = "PROTOMOLT_REPLAY_JOURNAL_EXIT_TEST";
    if let Some(path) = std::env::var_os(ENV) {
        let journal = ReplayJournal::create(std::path::Path::new(&path), binding()).unwrap();
        let receipt = accept(&journal, &first("exit")).unwrap();
        assert!(receipt.accepted && receipt.durable && !receipt.searchable);
        std::process::exit(0); // No journal/database destructor runs.
    }
    let dir = Directory::new("exit");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "replay_journal::journal_recovery_tests::accepted_receipt_survives_process_exit_without_drop",
            "--nocapture",
        ])
        .env(ENV, dir.path())
        .status_guarded()
        .unwrap();
    assert!(status.success());
    let journal = ReplayJournal::open(&dir.path(), binding()).unwrap();
    assert!(accept(&journal, &first("exit")).unwrap().replayed);
    assert_eq!(journal.read(&page(0)).unwrap().frames, [first("exit")]);
}

#[test]
fn missing_or_corrupted_history_is_never_adopted_as_an_empty_journal() {
    use redb::ReadableTable;
    for case in 0..3 {
        let dir = Directory::new("corrupt");
        let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
        accept(&journal, &first("doc")).unwrap();
        drop(journal);
        let db = redb::Database::open(dir.path()).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut frames = tx
                .open_table(redb::TableDefinition::<u64, &[u8]>::new("frames"))
                .unwrap();
            match case {
                0 => {
                    frames.remove(1).unwrap();
                }
                1 => {
                    let mut frame: pb::ReplayJournalFrame =
                        pb::ReplayJournalFrame::decode(frames.get(1).unwrap().unwrap().value())
                            .unwrap();
                    frame.record = record(11, "different");
                    frames.insert(1, frame.encode_to_vec().as_slice()).unwrap();
                }
                _ => {
                    let mut meta = tx
                        .open_table(redb::TableDefinition::<&str, &[u8]>::new("metadata"))
                        .unwrap();
                    meta.remove("header").unwrap();
                }
            }
        }
        tx.commit().unwrap();
        drop(db);
        assert!(ReplayJournal::open(&dir.path(), binding()).is_err());
    }
    let dir = Directory::new("empty");
    std::fs::write(dir.path(), []).unwrap();
    assert_eq!(
        ReplayJournal::open(&dir.path(), binding())
            .err()
            .unwrap()
            .code(),
        Code::DataLoss
    );
    assert_eq!(std::fs::metadata(dir.path()).unwrap().len(), 0);
}

#[test]
fn legal_wal_encodings_and_unknown_fields_are_preserved_without_normalizing() {
    let encodings = [
        vec![0x08, 0x01, 0x30, 0x0b, 0x22, 0x00],
        vec![0x22, 0x00, 0x30, 0x0b, 0x08, 0x01],
        vec![0x08, 0x81, 0x00, 0x22, 0x00, 0x30, 0x0b],
        vec![
            0x08, 0x01, 0x22, 0x00, 0x30, 0x0b, 0xfa, 0x07, 0x02, 0x00, 0xff,
        ],
    ];
    let semantic = pb::wal::WalRecord::decode(encodings[0].as_slice()).unwrap();
    let mut digests = std::collections::HashSet::new();
    for bytes in encodings {
        assert_eq!(
            pb::wal::WalRecord::decode(bytes.as_slice()).unwrap(),
            semantic
        );
        let dir = Directory::new("wire");
        let journal = ReplayJournal::create(&dir.path(), binding()).unwrap();
        let frame = seal_frame(
            &binding(),
            1,
            binding_digest(&binding()).unwrap(),
            bytes.clone(),
        )
        .unwrap();
        assert!(
            digests.insert(frame.sha256.clone()),
            "original byte identity is distinct"
        );
        accept(&journal, &frame).unwrap();
        drop(journal);
        let journal = ReplayJournal::open(&dir.path(), binding()).unwrap();
        assert_eq!(journal.read(&page(0)).unwrap().frames[0].record, bytes);
        assert!(accept(&journal, &frame).unwrap().replayed);
    }
}

#[test]
fn digest_matches_independently_encoded_contract_fixture() {
    // Fixture uses ascending tags, shortest varints, omitted default scalars,
    // and Python hashlib SHA-256, independently of Prost and the Rust hasher.
    let expected_binding = vec![
        250, 87, 157, 11, 13, 136, 3, 250, 148, 255, 191, 174, 104, 197, 61, 217, 77, 2, 46, 59,
        40, 46, 214, 141, 228, 240, 72, 89, 41, 8, 27, 193,
    ];
    assert_eq!(binding_digest(&binding()).unwrap(), expected_binding);
    let frame = seal_frame(
        &binding(),
        1,
        expected_binding,
        vec![0x22, 0, 0x30, 0x0b, 8, 1],
    )
    .unwrap();
    assert_eq!(
        frame.sha256,
        vec![
            50, 252, 35, 84, 2, 103, 13, 228, 166, 209, 35, 163, 118, 225, 104, 222, 94, 0, 27,
            167, 37, 181, 67, 57, 102, 11, 203, 162, 238, 31, 43, 252
        ]
    );
}
