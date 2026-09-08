use super::import_tests::key;
use super::map_feed_tests::{imported, prepare};
use super::*;
use crate::relay::MapSource;
use tonic::Code;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_relay_map_follows_committed_revisions_and_refuses_no_state() {
    let (_dir, authority, store, revision) = imported("relay-map");
    // A resource with no applied control state is a named refusal at
    // attach, never an empty map.
    let absent = {
        let store = store.clone();
        AuthorityMapSource::attach(move || Ok(store.clone()), "alice", &key("music"))
    };
    assert_eq!(absent.err().unwrap().code(), Code::NotFound);
    let source = {
        let store = store.clone();
        AuthorityMapSource::attach(move || Ok(store.clone()), "alice", &key("books")).unwrap()
    };
    let mut changes = source.changes();
    let current = source.current();
    assert_eq!(current.control_revision, revision);
    assert_eq!(current.topology_generation, 3);
    assert_eq!(*changes.borrow_and_update(), revision);
    // Routes come in committed order with their placement codes, and the
    // tree is the validated placement of that generation.
    let placement = current.map.placement.as_ref().expect("tree");
    assert_eq!(current.map.routes.len(), 2);
    for (route, name) in current.map.routes.iter().zip(["old", "rest"]) {
        assert_eq!(
            route.placement,
            Some(placement.leaf_by_name(name).unwrap().code)
        );
        assert!(route.hash_range.is_some());
    }
    assert!(source.fault().is_none());
    // A control change advances the revision; the reading is replaced and
    // the change receiver moves, with the generation unchanged.
    assert_eq!(
        store
            .execute("alice", &prepare(&authority, revision))
            .unwrap()
            .code,
        0
    );
    tokio::time::timeout(std::time::Duration::from_secs(10), changes.changed())
        .await
        .expect("change within 10 s")
        .unwrap();
    assert_eq!(*changes.borrow_and_update(), revision + 1);
    let next = source.current();
    assert_eq!(next.control_revision, revision + 1);
    assert_eq!(next.topology_generation, 3);
    assert_eq!(next.map.routes, current.map.routes);
    assert!(source.fault().is_none());
    // A principal without Admin on the resource attaches nothing.
    let denied = {
        let store = store.clone();
        AuthorityMapSource::attach(move || Ok(store.clone()), "carol", &key("books"))
    };
    assert_eq!(denied.err().unwrap().code(), Code::PermissionDenied);
}
