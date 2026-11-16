use super::*;

#[test]
fn envelopes_round_trip_and_malformed_ones_refuse() {
    let id = log_id(3, 7, 42);
    assert_eq!(log_id_from_proto(&log_id_to_proto(&id)), id);
    for vote in [Vote::new(3, 7), Vote::new_committed(4, 2)] {
        assert_eq!(vote_from_proto(&vote_to_proto(&vote)), vote);
    }
    let joint = Membership::new(
        vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([2, 3, 4])],
        BTreeMap::from([
            (1, BasicNode { addr: "a".into() }),
            (2, BasicNode { addr: "b".into() }),
            (3, BasicNode { addr: "c".into() }),
            (4, BasicNode { addr: "d".into() }),
            (
                9,
                BasicNode {
                    addr: "learner".into(),
                },
            ),
        ]),
    );
    assert_eq!(
        membership_from_proto(&membership_to_proto(&joint)).unwrap(),
        joint
    );
    let stored = openraft::StoredMembership::new(Some(id), joint.clone());
    assert_eq!(
        stored_membership_from_proto(&stored_membership_to_proto(&stored)).unwrap(),
        stored
    );
    let command = prepare(&identity(7), "phone", "prepare", 1);
    for entry in [
        Entry {
            log_id: id,
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: id,
            payload: EntryPayload::Normal(proposal("alice", &command)),
        },
        Entry {
            log_id: id,
            payload: EntryPayload::Membership(joint.clone()),
        },
    ] {
        let back = entry_from_proto(entry_to_proto(&entry)).unwrap();
        assert_eq!(back.log_id, entry.log_id);
        assert_eq!(
            format!("{:?}", back.payload),
            format!("{:?}", entry.payload)
        );
    }
    malformed_envelopes_refuse();
    let signature = ImageSignature {
        length: 4096,
        sha256: [0xab; 32],
    };
    let snapshot_id = format_snapshot_id(Some(&id), &signature);
    assert_eq!(parse_snapshot_id(&snapshot_id).unwrap(), signature);
    assert!(parse_snapshot_id("42-4096-nothex").is_err());
}

/// A voter with no node record, unsorted ids and an entry without a log
/// id are all data loss, never defaults.
fn malformed_envelopes_refuse() {
    let orphan = RaftMembership {
        configs: vec![RaftVoterSet {
            node_ids: vec![1, 5],
        }],
        nodes: vec![RaftNode {
            node_id: 1,
            addr: "a".into(),
        }],
    };
    assert_eq!(
        membership_from_proto(&orphan).err().unwrap().code(),
        Code::DataLoss
    );
    let unsorted = RaftMembership {
        configs: vec![RaftVoterSet {
            node_ids: vec![2, 1],
        }],
        nodes: vec![
            RaftNode {
                node_id: 1,
                addr: "a".into(),
            },
            RaftNode {
                node_id: 2,
                addr: "b".into(),
            },
        ],
    };
    assert_eq!(
        membership_from_proto(&unsorted).err().unwrap().code(),
        Code::DataLoss
    );
    assert_eq!(
        entry_from_proto(RaftEntry {
            log_id: None,
            payload: Some(Payload::Blank(true)),
        })
        .err()
        .unwrap()
        .code(),
        Code::DataLoss
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_log_store_keeps_votes_consecutive_entries_and_boundaries() {
    let dir = Directory::new("log");
    let group = identity(7);
    let path = dir.0.join("raft-log.redb");
    let mut store = RaftLogStore::create(&path, &group, 1).unwrap();
    assert_eq!(store.read_vote().await.unwrap(), None);
    store.save_vote(&Vote::new(2, 1)).await.unwrap();
    assert_eq!(store.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
    let command = prepare(&group, "phone", "prepare", 1);
    let entries: Vec<Entry<ControlRaft>> = (1..=3)
        .map(|index| Entry {
            log_id: log_id(2, 1, index),
            payload: if index == 1 {
                EntryPayload::Blank
            } else {
                EntryPayload::Normal(proposal("alice", &command))
            },
        })
        .collect();
    store.append_sync(entries.clone()).unwrap();
    let read = store.try_get_log_entries(2..=3).await.unwrap();
    assert_eq!(read.len(), 2);
    assert_eq!(read[0].log_id, log_id(2, 1, 2));
    // A hole is refused by name.
    let hole = store.append_sync(vec![Entry {
        log_id: log_id(2, 1, 9),
        payload: EntryPayload::Blank,
    }]);
    assert!(hole.unwrap_err().to_string().contains("hole"));
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id, Some(log_id(2, 1, 3)));
    assert_eq!(state.last_purged_log_id, None);
    store.truncate_sync(log_id(2, 1, 3)).unwrap();
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(2, 1, 2))
    );
    store.save_committed(Some(log_id(2, 1, 2))).await.unwrap();
    store.purge_sync(log_id(2, 1, 1)).unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(2, 1, 1)));
    assert_eq!(state.last_log_id, Some(log_id(2, 1, 2)));
    assert_eq!(store.entries().unwrap().len(), 1);
    drop(store);
    // Reopen keeps everything; another node or group refuses.
    assert_eq!(
        RaftLogStore::open(&path, &group, 2).err().unwrap().code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        RaftLogStore::open(&path, &identity(9), 1)
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    let mut reopened = RaftLogStore::open(&path, &group, 1).unwrap();
    assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(2, 1)));
    assert_eq!(
        reopened.read_committed().await.unwrap(),
        Some(log_id(2, 1, 2))
    );
    let state = reopened.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(2, 1, 1)));
    assert_eq!(state.last_log_id, Some(log_id(2, 1, 2)));
    assert_eq!(
        RaftLogStore::open(&dir.0.join("missing.redb"), &group, 1)
            .err()
            .unwrap()
            .code(),
        Code::NotFound
    );
}
