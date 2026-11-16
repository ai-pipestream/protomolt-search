use super::*;

fn review_two_resource_installs(dir: &Directory) -> SourceAuthorityStore {
    let authority = identity(7);
    let store = store(dir, 7, &limits(64 << 10, 64));
    install(&store, &authority, state(2), 1);
    let mut empty = state(2);
    empty.nodes.clear();
    empty.replicas.clear();
    empty.actions.clear();
    empty.completed_actions.clear();
    let mut second = command(&authority, "install-music", 2, empty);
    second.key = Some(key("music"));
    let decision = store.install_control("alice", &second).unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    store
}

#[test]
fn review_followup_recovery_accepts_two_installs_including_zero_fact_resource() {
    let dir = Directory::new("review-valid-two-installs");
    let authority = identity(7);
    let store = review_two_resource_installs(&dir);
    let books = store.control_snapshot("alice", &key("books")).unwrap();
    let music = store.control_snapshot("alice", &key("music")).unwrap();
    assert!(!books.nodes.is_empty());
    assert!(music.nodes.is_empty() && music.replicas.is_empty());
    assert!(music.actions.is_empty() && music.completed_actions.is_empty());
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(
        reopened.control_snapshot("alice", &key("books")).unwrap(),
        books
    );
    assert_eq!(
        reopened.control_snapshot("alice", &key("music")).unwrap(),
        music
    );
}

#[test]
fn review_followup_recovery_rejects_recounted_orphan_hidden_by_zero_fact_install() {
    let dir = Directory::new("review-orphan-among-two-installs");
    let store = review_two_resource_installs(&dir);
    let tx = store.inner.database().begin_write().unwrap();
    let orphan_key = ControlActionKey {
        key: Some(key("film")),
        action_id: 7,
    }
    .encode_to_vec();
    let orphan_value = ControlCompletedAction { format_version: 1 }.encode_to_vec();
    {
        let mut completed = tx.open_table(COMPLETED).unwrap();
        assert!(completed
            .insert(orphan_key.as_slice(), orphan_value.as_slice())
            .unwrap()
            .is_none());
        let mut meta = tx.open_table(META).unwrap();
        let mut control: ControlStoreHeader =
            contract::decode(meta.get(CONTROL_HEADER).unwrap().unwrap().value()).unwrap();
        control.completed_count += 1;
        meta.insert(CONTROL_HEADER, control.encode_to_vec().as_slice())
            .unwrap();
        let mut source: SourceAuthorityHeader =
            contract::decode(meta.get("header").unwrap().unwrap().value()).unwrap();
        source.payload_bytes += (orphan_key.len() + orphan_value.len()) as u64;
        meta.insert("header", source.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    drop(store);
    let error = match SourceAuthorityStore::open(&dir.authority(), &identity(7)) {
        Err(error) => error,
        Ok(_) => panic!("recovery accepted an uninstalled fact resource"),
    };
    assert_eq!(error.code(), Code::DataLoss);
    assert!(error
        .message()
        .contains("installed facts exist without a control row"));
}

fn review_snapshot(name: &str, state: ControlInstallState) -> ControlCollectionSnapshot {
    let dir = Directory::new(name);
    let authority = identity(7);
    let store = store(&dir, 7, &limits(64 << 10, 64));
    install(&store, &authority, state, 1);
    let snapshot = store.control_snapshot("alice", &key("books")).unwrap();
    let mut digest_input = snapshot.clone();
    digest_input.digest.clear();
    assert_eq!(
        snapshot.digest,
        hash(
            b"protomolt.control-snapshot.v1\0",
            &digest_input.encode_to_vec()
        )
    );
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(
        reopened.control_snapshot("alice", &key("books")).unwrap(),
        snapshot
    );
    snapshot
}

#[test]
fn review_followup_snapshot_nodes_use_node_id_order_across_unequal_lengths() {
    let mut state = state(2);
    state.nodes[0].node_id = "a-much-longer-node".into();
    state.nodes[1].node_id = "b".into();
    let snapshot = review_snapshot("review-node-order", state);
    assert_eq!(
        snapshot
            .nodes
            .iter()
            .map(|n| n.node_id.as_str())
            .collect::<Vec<_>>(),
        ["a-much-longer-node", "b"]
    );
}

#[test]
fn review_followup_snapshot_replicas_use_decoded_replica_key_order() {
    let mut state = state(2);
    state.replicas[0].shard_id = "a-much-longer-shard".into();
    state.replicas[1].shard_id = "b".into();
    let want: Vec<_> = state
        .replicas
        .iter()
        .map(|r| replica_key(&r.shard_id, &r.node_id))
        .collect();
    let snapshot = review_snapshot("review-replica-order", state);
    let got: Vec<_> = snapshot
        .replicas
        .iter()
        .map(|r| {
            let r = r.replica.as_ref().unwrap();
            replica_key(&r.shard_id, &r.node_id)
        })
        .collect();
    assert_eq!(got, want);
}

#[test]
fn review_followup_snapshot_actions_use_action_id_order_and_preserve_pending_order() {
    let mut state = state(2);
    let mut first = state.actions[0].clone();
    first.action_id = 300;
    let mut second = first.clone();
    second.action_id = 200;
    state.actions = vec![first, second];
    let snapshot = review_snapshot("review-action-order", state);
    assert_eq!(
        snapshot
            .actions
            .iter()
            .map(|a| a.action.as_ref().unwrap().action_id)
            .collect::<Vec<_>>(),
        [200, 300]
    );
    assert_eq!(
        snapshot.actions.iter().map(|a| a.order).collect::<Vec<_>>(),
        [1, 0]
    );
}

#[test]
fn review_followup_snapshot_completed_ids_use_numeric_order_across_varint_lengths() {
    let mut state = state(2);
    state.completed_actions = vec![3, 200, 300];
    let snapshot = review_snapshot("review-completed-order", state);
    assert_eq!(snapshot.completed_actions, [3, 200, 300]);
}
