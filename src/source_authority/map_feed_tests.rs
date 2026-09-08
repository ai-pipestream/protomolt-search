use super::import_tests::{
    checkpoint_of, identity, key, legacy, limits, payload, retire, run_import, store, supplement,
    Directory,
};
use super::*;
use crate::pb::storage::legacy_control_import_supplement::Placement;
use crate::pb::{PlacementNode, PlacementTree};
use tonic::Code;

fn tree() -> PlacementTree {
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

fn imported(
    name: &str,
) -> (
    Directory,
    SourceAuthorityIdentity,
    SourceAuthorityStore,
    u64,
) {
    let dir = Directory::new(name);
    let limits = limits(64 << 10, 128);
    let authority = identity(7);
    let store = store(&dir, 7, &limits);
    let legacy = legacy(&dir, 2);
    let (retired, _) = retire(&store, &legacy, 1);
    let checkpoint = checkpoint_of(&retired);
    let placement = crate::placement::Placement::validate(
        &crate::placement::PlacementTreeConfig::from_proto(&tree()),
    )
    .unwrap();
    let mut with_tree = supplement(&checkpoint);
    with_tree.placement = Some(Placement::Tree(tree()));
    with_tree.route_codes = ["old", "rest"]
        .map(|name| PlacementRouteCode {
            has_placement: true,
            placement: placement.leaf_by_name(name).unwrap().code as u64,
        })
        .to_vec();
    let payload = payload(&retired, with_tree);
    let (_, revision, _) = run_import(&store, &authority, &limits, &retired, &payload, 1);
    (dir, authority, store, revision)
}

fn prepare(authority: &SourceAuthorityIdentity, revision: u64) -> SourceAuthorityCommand {
    SourceAuthorityCommand {
        format_version: 1,
        authority: Some(authority.clone()),
        key: Some(LogicalSourceOwner {
            workspace: "workspace-a".into(),
            collection: "books".into(),
            owner_id: b"phone-owner".to_vec(),
        }),
        command_id: b"prepare".to_vec(),
        expected_control_revision: revision,
        expected_policy_revision: 1,
        expected_ownership_generation: 0,
        action: Some(Action::Prepare(PrepareSourceOwner {
            workflow_id: b"installation-one".to_vec(),
            target: Some(SourceStorageTarget {
                node_id: "node/α".into(),
                storage_incarnation: vec![41; 16],
                history_id: vec![42; 16],
                residency: SourceResidency::Server as i32,
                resident_device_id: String::new(),
            }),
        })),
    }
}

#[test]
fn the_map_is_produced_from_committed_rows_and_the_feed_wakes_per_revision() {
    let (dir, authority, store, revision) = imported("map");
    let mut applied = store.subscribe_applied();
    assert_eq!(*applied.borrow_and_update(), revision);
    assert_eq!(
        store
            .published_map("alice", &key("music"))
            .err()
            .unwrap()
            .code(),
        Code::NotFound
    );
    let map = store.published_map("alice", &key("books")).unwrap();
    assert_eq!(map.control_revision, revision);
    assert_eq!(map.topology_generation, 3);
    assert_eq!(map.routes.len(), 2);
    assert!(map.codes_available);
    assert_eq!(map.codes.len(), 2);
    assert_eq!(map.tree, Some(tree()));
    assert_eq!(map.history_generations, [1, 2]);
    assert_eq!(map.replicas.len(), 2);
    assert_eq!(
        map.nodes
            .iter()
            .map(|n| n.node_id.as_str())
            .collect::<Vec<_>>(),
        ["node z", "node/α"]
    );
    assert_eq!(map.nodes[0].residency, SourceResidency::DeviceLocal as i32);
    assert_eq!(map.nodes[0].failure_domain, "rack/ß");
    assert!(map.owners.is_empty());
    assert_eq!(map.digest.len(), 32);
    assert_eq!(store.published_map("alice", &key("books")).unwrap(), map);
    // An owner preparation advances the revision, not the generation, and
    // the feed wakes; the owner appears without a fence.
    assert_eq!(
        store
            .execute("alice", &prepare(&authority, revision))
            .unwrap()
            .code,
        0
    );
    assert!(applied.has_changed().unwrap());
    assert_eq!(*applied.borrow_and_update(), revision + 1);
    let next = store.published_map("alice", &key("books")).unwrap();
    assert_eq!(next.control_revision, revision + 1);
    assert_eq!(next.topology_generation, 3);
    assert_eq!(next.routes, map.routes);
    assert_eq!(next.owners.len(), 1);
    assert_eq!(next.owners[0].owner_id, b"phone-owner");
    assert_eq!(
        next.owners[0].phase,
        PreparedSourceOwnerPhase::Prepared as i32
    );
    assert_eq!(next.owners[0].write_epoch, 0);
    assert_ne!(next.digest, map.digest);
    // A rejected command commits no change and wakes nobody.
    assert_eq!(
        store
            .execute("alice", &{
                let mut c = prepare(&authority, revision);
                c.command_id = b"stale".to_vec();
                c
            })
            .unwrap()
            .code,
        Code::FailedPrecondition as u32
    );
    assert!(!applied.has_changed().unwrap());
    // A revoked reader cannot read the map; reopen reproduces it exactly.
    assert_eq!(
        store
            .published_map("carol", &key("books"))
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );
    drop(applied);
    drop(store);
    let reopened = SourceAuthorityStore::open(&dir.authority(), &authority).unwrap();
    assert_eq!(*reopened.subscribe_applied().borrow(), revision + 1);
    assert_eq!(
        reopened.published_map("alice", &key("books")).unwrap(),
        next
    );
}

#[test]
fn the_consumer_refuses_forged_stale_and_conflicting_frames() {
    let (_dir, authority, store, revision) = imported("consumer");
    let first = store.published_map("alice", &key("books")).unwrap();
    let mut consumer = MapConsumer::new();
    assert!(consumer.offer(first.clone()).unwrap());
    assert_eq!(consumer.held(), Some(&first));
    // Same frame again: nothing changes.
    assert!(!consumer.offer(first.clone()).unwrap());
    // Same revision, different content: an error, not a swap.
    let mut forged = first.clone();
    forged.routes[0].hash_hi += 1;
    forged.digest = super::map_feed::digest(&forged);
    let error = consumer.offer(forged).err().unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("different content"), "{error}");
    // A frame whose digest does not match its bytes.
    let mut bad_digest = first.clone();
    bad_digest.control_revision += 1;
    let error = consumer.offer(bad_digest).err().unwrap();
    assert!(error.message().contains("digest differs"), "{error}");
    // A newer revision with the same generation and different routes.
    let mut renamed = first.clone();
    renamed.control_revision += 1;
    renamed.routes[1].addr = "https://elsewhere:9443/".into();
    renamed.digest = super::map_feed::digest(&renamed);
    let error = consumer.offer(renamed).err().unwrap();
    assert!(error.message().contains("names a different map"), "{error}");
    // A newer revision with a generation behind the held one.
    let mut behind = first.clone();
    behind.control_revision += 1;
    behind.topology_generation = 2;
    behind.digest = super::map_feed::digest(&behind);
    let error = consumer.offer(behind).err().unwrap();
    assert!(error.message().contains("behind the held"), "{error}");
    // Another resource or authority never replaces the held map.
    let mut other = first.clone();
    other.key.as_mut().unwrap().collection = "music".into();
    other.control_revision += 1;
    other.digest = super::map_feed::digest(&other);
    assert!(consumer
        .offer(other)
        .err()
        .unwrap()
        .message()
        .contains("another authority or resource"));
    // The real next frame (a capacity-only revision) is accepted with the
    // same generation; the previous one is then ignored as older.
    assert_eq!(
        store
            .execute("alice", &prepare(&authority, revision))
            .unwrap()
            .code,
        0
    );
    let second = store.published_map("alice", &key("books")).unwrap();
    assert!(consumer.offer(second.clone()).unwrap());
    assert_eq!(consumer.held().unwrap().control_revision, revision + 1);
    assert!(!consumer.offer(first).unwrap());
    assert_eq!(consumer.held(), Some(&second));
}
