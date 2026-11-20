//! Topology validation, hot snapshots, cutover, and stable key routing.

use super::*;

fn route(addr: &str, lo: u64, hi: u64) -> TopologyRoute {
    TopologyRoute {
        addr: addr.to_string(),
        replica: None,
        hash_range: Some((lo, hi)),
        placement: None,
    }
}

#[test]
fn topology_refuses_ragged_or_incomplete_hash_space() {
    assert!(
        build_topology(1, vec![route("a", 0, 9), route("b", 11, u64::MAX)], None)
            .err()
            .expect("gap must be refused")
            .contains("gap or overlap")
    );
    assert!(
        build_topology(1, vec![route("a", 0, 10), route("b", 10, u64::MAX)], None)
            .err()
            .expect("overlap must be refused")
            .contains("gap or overlap")
    );
    assert!(build_topology(
        1,
        vec![
            route("a", 0, u64::MAX),
            TopologyRoute {
                addr: "b".to_string(),
                replica: None,
                hash_range: None,
                placement: None,
            }
        ],
        None
    )
    .err()
    .expect("ragged ranges must be refused")
    .contains("every shard or none"));
    assert!(build_topology(
        1,
        vec![
            TopologyRoute {
                addr: "a".into(),
                replica: Some("b".into()),
                hash_range: Some((0, 9)),
                placement: None,
            },
            route("b", 10, u64::MAX),
        ],
        None,
    )
    .err()
    .expect("a replica cannot also serve another logical shard")
    .contains("duplicate topology endpoint"));
}

#[test]
fn hot_topology_snapshots_one_generation_per_request() {
    let coordinator = CoordinatorServiceImpl::new(vec!["old".to_string()])
        .with_topology_generation(4)
        .with_hot_topology(vec![Some((0, u64::MAX))])
        .unwrap();
    let old = coordinator.request_snapshot().unwrap();

    coordinator
        .reload_topology(5, vec![route("new", 0, u64::MAX)], None)
        .unwrap();
    let new = coordinator.request_snapshot().unwrap();

    assert_eq!(old.topology_generation, 4);
    assert_eq!(old.node_addrs, vec!["old"]);
    assert_eq!(new.topology_generation, 5);
    assert_eq!(new.node_addrs, vec!["new"]);
    assert_eq!(coordinator.current_topology_generation(), 5);
    assert!(coordinator
        .reload_topology(5, vec![route("newer", 0, u64::MAX)], None)
        .unwrap_err()
        .contains("must increase"));
}

#[tokio::test]
async fn cutover_barrier_publishes_a_new_shard_count_atomically() {
    let coordinator = CoordinatorServiceImpl::new(vec!["old".to_string()])
        .with_topology_generation(4)
        .with_hot_topology(vec![Some((0, u64::MAX))])
        .unwrap();
    let frozen = SearchService::freeze_topology_writes(
        &coordinator,
        Request::new(FreezeTopologyWritesRequest {
            collection: String::new(),
            required_topology_generation: 4,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(coordinator
        .reload_topology(5, vec![route("other", 0, u64::MAX)], None)
        .unwrap_err()
        .contains("frozen writes"));

    let split = u64::MAX / 2;
    SearchService::publish_topology(
        &coordinator,
        Request::new(PublishTopologyRequest {
            collection: String::new(),
            placement: None,
            cutover_token: frozen.cutover_token,
            generation: 5,
            shards: vec![
                crate::pb::PublishedTopologyShard {
                    addr: "a:50051".into(),
                    replica: String::new(),
                    hash_lo: 0,
                    hash_hi: split,
                    has_placement: false,
                    placement: 0,
                },
                crate::pb::PublishedTopologyShard {
                    addr: "b:50051".into(),
                    replica: String::new(),
                    hash_lo: split + 1,
                    hash_hi: u64::MAX,
                    has_placement: false,
                    placement: 0,
                },
            ],
        }),
    )
    .await
    .unwrap();
    assert_eq!(coordinator.current_topology_generation(), 5);
    assert_eq!(coordinator.current_topology_routes().len(), 2);
    let (_, routed) = coordinator.route_stable_key(b"stable-product-id").unwrap();
    assert!(routed < 2);

    let frozen = SearchService::freeze_topology_writes(
        &coordinator,
        Request::new(FreezeTopologyWritesRequest {
            collection: String::new(),
            required_topology_generation: 5,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let aborted = SearchService::abort_topology_cutover(
        &coordinator,
        Request::new(AbortTopologyCutoverRequest {
            collection: String::new(),
            cutover_token: frozen.cutover_token,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(aborted.topology_generation, 5);
}

#[test]
fn stable_product_keys_route_deterministically() {
    let split = u64::MAX / 2;
    let coordinator = CoordinatorServiceImpl::new(vec!["a".to_string(), "b".to_string()])
        .with_topology_generation(9)
        .with_hot_topology(vec![Some((0, split)), Some((split + 1, u64::MAX))])
        .unwrap();
    let key = b"courtlistener:opinion:123/chunk:7";
    let expected = usize::from(stable_routing_hash(key) > split);
    assert_eq!(coordinator.route_stable_key(key), Ok((9, expected)));
    assert_eq!(coordinator.route_stable_key(key), Ok((9, expected)));
    assert!(coordinator
        .route_stable_key(b"")
        .unwrap_err()
        .contains("empty"));
}
