//! The migration and disaster-recovery exercise of
//! docs/raft-control-design.md, "Implementation sequence and acceptance",
//! step 5, on disposable state: the fleet's own root map becomes a legacy
//! control plane, is retired and imported into a bootstrapped three-voter
//! group, the committed map is what the relay would route on, and the
//! group then loses its leader with a write in flight, serves a member an
//! image with changed bytes and replaces a lost member from a prepared
//! directory, restarts from cold, and recovers its retirement record.
//! Every step prints a timed line so the run is its own evidence
//! (docs/raft-migration-exercise.md).
#![cfg(all(feature = "raft", feature = "tls", feature = "fault-injection"))]

mod control_adversarial;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use control_adversarial::{kit, raft_kit};
use pipestream_search::authorization::{AccessPermit, Authorizer, PolicyAuthority};
use pipestream_search::config::{load_shard_map, RaftManagedCatalog};
use pipestream_search::control_plane::DurableControlPlane;
use pipestream_search::coordinator::TopologyRoute;
use pipestream_search::document_catalog::{
    Acceptance, AccessControlledCatalog, ActiveManagedCatalog,
};
use pipestream_search::document_write_service::hosted::recover_catalogs;
use pipestream_search::pb::storage::legacy_control_import_supplement::Placement;
use pipestream_search::pb::storage::{
    ConfirmSourceOwnerReady, PlacementRouteCode, SourceAuthorityIdentity, SourceOwnerCompletion,
};
use pipestream_search::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, AccessAction, AccessPolicy,
    ProtobufSource,
};
use pipestream_search::raft::RaftHost;
use pipestream_search::relay::MapSource;
use pipestream_search::source_authority::AuthorityMapSource;
use raft_kit::Voters;
use tonic::Code;

const FLEET_MAP: &str = include_str!("fixtures/fleet/root-map-v10.toml");
const WORKFLOW: &[u8] = b"migration-owner";

static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn mark(step: &str) {
    let start = START.get_or_init(Instant::now);
    eprintln!("[exercise +{:>7.3}s] {step}", start.elapsed().as_secs_f64());
}

fn policy() -> AccessPolicy {
    let mut policy = kit::policy();
    for grant in &mut policy.grants {
        if grant.principal == "alice" {
            grant.actions.push(AccessAction::Ingest as i32);
        }
    }
    policy
}

fn catalog_write(document_key: &[u8], operation_id: &[u8]) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: document_key.to_vec(),
        operation_id: operation_id.to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: include_bytes!("fixtures/protobuf-semantics/descriptor.bin").to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, 7],
        })),
        ..Default::default()
    }
}

fn catalog_fixture(dir: &kit::TestDir) -> (AccessControlledCatalog, Vec<u8>) {
    let authorizer: Arc<dyn Authorizer> = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let admin = AccessPermit::acquire(
        authorizer.clone(),
        "alice",
        kit::COLLECTION,
        AccessAction::Admin,
    )
    .unwrap();
    let ingest =
        AccessPermit::acquire(authorizer, "alice", kit::COLLECTION, AccessAction::Ingest).unwrap();
    let catalog = AccessControlledCatalog::create(
        &dir.path().join("catalog.redb"),
        &pipestream_search::pb::storage::SourceResourceBinding {
            format_version: 1,
            workspace: kit::WORKSPACE.into(),
            collection: kit::COLLECTION.into(),
        },
        &admin,
    )
    .unwrap();
    let receipt = catalog
        .accept(&ingest, &catalog_write(b"document-one", b"accept-one"))
        .unwrap();
    (catalog, receipt.history_id)
}

fn confirm_command(
    authority: &SourceAuthorityIdentity,
    control_revision: u64,
    completion: SourceOwnerCompletion,
) -> pipestream_search::pb::storage::SourceAuthorityCommand {
    kit::source_command(
        authority,
        &kit::owner_key(),
        "confirm-ready",
        control_revision,
        1,
        1,
        pipestream_search::pb::storage::source_authority_command::Action::ConfirmReady(
            ConfirmSourceOwnerReady {
                workflow_id: WORKFLOW.to_vec(),
                completion: Some(completion),
            },
        ),
    )
}

/// Prepare, bind, confirm and activate one managed source on the leader.
async fn bridge_active(
    voters: &mut Voters,
    leader: u64,
    dir: &kit::TestDir,
) -> ActiveManagedCatalog {
    let authority = voters.group.clone();
    let (catalog, history_id) = catalog_fixture(dir);
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "prepare-owner",
        voters.revision,
        1,
        0,
        kit::prepare_action_history(WORKFLOW, history_id),
    );
    let decision = voters
        .host(leader)
        .propose_command("alice", &prepare)
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    voters.revision += 1;
    let preparation = decision.owner.clone().unwrap();
    let store = voters.host(leader).store().unwrap();
    let managed = voters
        .host(leader)
        .with_admission("alice", |admission| {
            catalog.bind_prepared_owner(admission, &store, &preparation, 1 << 20)
        })
        .await
        .unwrap();
    let verified = managed.completion().unwrap();
    let confirm = confirm_command(&authority, voters.revision, verified.completion().clone());
    let confirmed = voters
        .host(leader)
        .propose_confirm_ready("alice", &confirm, &managed.completion().unwrap())
        .await
        .unwrap();
    assert_eq!(confirmed.code, 0, "{}", confirmed.message);
    voters.revision += 1;
    let activate = kit::source_command(
        &authority,
        &kit::owner_key(),
        "activate-owner",
        voters.revision,
        1,
        1,
        kit::activate_action(WORKFLOW),
    );
    let activated = voters
        .host(leader)
        .propose_command("alice", &activate)
        .await
        .unwrap();
    assert_eq!(activated.code, 0, "{}", activated.message);
    voters.revision += 1;
    voters
        .host(leader)
        .with_admission("alice", |admission| managed.activate(admission))
        .await
        .unwrap()
}

/// The fleet's root map as a legacy control topology: one route per shard
/// in file order with its placement code, and stable-key hash ranges tiled
/// per leaf, since the legacy plane requires ranges and the fleet's map
/// (slot offsets and codes) carries none.
fn fleet_routes() -> (
    Vec<TopologyRoute>,
    pipestream_search::placement::PlacementTreeConfig,
    u64,
) {
    let map = {
        let path = std::env::temp_dir().join(format!("root-map-v10-{}.toml", std::process::id()));
        std::fs::write(&path, FLEET_MAP).unwrap();
        let map = load_shard_map(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        map
    };
    let tree = map
        .placement
        .clone()
        .expect("the fleet map has a placement tree");
    let mut per_code: std::collections::BTreeMap<u64, Vec<usize>> = Default::default();
    for (index, shard) in map.shards.iter().enumerate() {
        per_code
            .entry(shard.placement.expect("every fleet shard has a code"))
            .or_default()
            .push(index);
    }
    let space = u64::MAX as u128 + 1;
    let mut routes: Vec<Option<TopologyRoute>> = vec![None; map.shards.len()];
    for members in per_code.values() {
        let count = members.len() as u128;
        for (position, index) in members.iter().enumerate() {
            let lo = (position as u128 * space / count) as u64;
            let hi = ((position as u128 + 1) * space / count - 1) as u64;
            let shard = &map.shards[*index];
            routes[*index] = Some(TopologyRoute {
                addr: shard.addr.clone(),
                replica: shard.replica.clone(),
                hash_range: Some((lo, hi)),
                placement: shard.placement.map(|code| code as i64),
            });
        }
    }
    (
        routes
            .into_iter()
            .map(|r| r.expect("every route tiled"))
            .collect(),
        tree,
        map.generation,
    )
}

/// Every import step through the leader, the revision tracker advanced.
async fn import(
    voters: &mut Voters,
    leader: u64,
    retired: &pipestream_search::control_plane::RetiredLegacyControl,
    payload: &[u8],
) {
    let authority = voters.group.clone();
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = kit::import_command(
        &authority,
        "import-begin",
        voters.revision,
        kit::WORKFLOW,
        kit::begin_action(retired, payload, chunk_bytes, chunk_count),
    );
    let decision = voters
        .host(leader)
        .propose_import("alice", &begin, Some(retired))
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    voters.revision += 1;
    for ordinal in 0..chunk_count {
        let chunk = kit::import_command(
            &authority,
            &format!("import-chunk-{ordinal}"),
            voters.revision,
            kit::WORKFLOW,
            kit::chunk_action(payload, chunk_bytes, ordinal),
        );
        let decision = voters
            .host(leader)
            .propose_import("alice", &chunk, None)
            .await
            .unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
        voters.revision += 1;
    }
    let commit = kit::import_command(
        &authority,
        "import-commit",
        voters.revision,
        kit::WORKFLOW,
        kit::commit_action(),
    );
    let decision = voters
        .host(leader)
        .propose_import("alice", &commit, None)
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    voters.revision += 1;
}

/// The committed map as the relay's map source would read it on `node`,
/// compared route by route with the fleet's map.
fn assert_fleet_map(voters: &Voters, node: u64, routes: &[TopologyRoute], generation: u64) {
    let source =
        AuthorityMapSource::attach(voters.host(node).store_handle(), "alice", &kit::key()).unwrap();
    let snapshot = source.current();
    assert_eq!(
        snapshot.topology_generation, generation,
        "node {node}: generation"
    );
    assert_eq!(
        snapshot.map.routes.len(),
        routes.len(),
        "node {node}: route count"
    );
    for (index, (got, want)) in snapshot.map.routes.iter().zip(routes).enumerate() {
        assert_eq!(got.addr, want.addr, "node {node}: route {index} address");
        assert_eq!(
            got.placement, want.placement,
            "node {node}: route {index} code"
        );
        assert_eq!(
            got.hash_range, want.hash_range,
            "node {node}: route {index} range"
        );
    }
    let placement = snapshot
        .map
        .placement
        .as_ref()
        .expect("the committed map has the tree");
    assert_eq!(placement.config().nodes.len(), 2, "node {node}: two leaves");
}

async fn write_through(
    voters: &Voters,
    node: u64,
    catalog: &ActiveManagedCatalog,
    key: &[u8],
    op: &[u8],
) -> pipestream_search::pb::DocumentWriteReceipt {
    let request = catalog_write(key, op);
    match voters
        .host(node)
        .with_admission("alice", |admission| catalog.accept(admission, &request))
        .await
        .unwrap()
    {
        Acceptance::Accepted(receipt) => receipt,
        Acceptance::Unconfirmed(receipt) => panic!("unconfirmed inside its lease: {receipt:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fleet_map_migrates_into_a_group_that_survives_the_exercise() {
    mark("start");
    let config = {
        let mut config = raft_kit::host_config();
        config.election_timeout_min_ms = 1_000;
        config.election_timeout_max_ms = 2_000;
        config.admission_lease_ms = 500;
        config
    };
    let lease = Duration::from_millis(config.admission_lease_ms);

    // 1. The fleet's map as a legacy control plane, quiesced (nothing serves
    //    it), and its checkpoint retired under Admin into a group of three.
    let (routes, tree, generation) = fleet_routes();
    assert_eq!(routes.len(), 7, "seven fleet routes");
    let legacy_dir = kit::TestDir::new("migration-legacy");
    let plane = DurableControlPlane::open(legacy_dir.legacy(), kit::control_policy())
        .unwrap()
        .with_collection(kit::COLLECTION)
        .unwrap();
    plane.bootstrap_topology(generation, &routes).unwrap();
    mark("legacy control plane bootstrapped from the fleet map");

    let mut cluster = raft_kit::three_voters_with("migration", &policy(), &config).await;
    let leader = cluster.leader().await;
    mark(&format!("three voters bootstrapped, leader {leader}"));
    let store = cluster.host(leader).store().unwrap();
    // Retirement is the legacy side's record; it commits nothing on the
    // group, so the revision tracker does not move.
    let (retired, request) = kit::retire(&store, &plane, cluster.revision);
    drop(store);
    let checkpoint = kit::checkpoint_of(&retired);
    let checkpoint_digest = pipestream_search::sha256::digest(retired.checkpoint_bytes());
    assert_eq!(
        checkpoint.state().topology.as_ref().unwrap().routes.len(),
        7
    );
    mark("legacy authority retired; the plane's clones are fenced");
    let plane_again =
        DurableControlPlane::open_existing(legacy_dir.legacy(), kit::control_policy());
    assert!(
        plane_again.is_err(),
        "a retired legacy path reopens as a writer"
    );
    drop(plane);

    // 2. Import with the fleet's placement tree and one code per route.
    let mut supplement = kit::supplement(&checkpoint);
    supplement.placement = Some(Placement::Tree(tree.to_proto()));
    supplement.route_codes = routes
        .iter()
        .map(|route| PlacementRouteCode {
            has_placement: true,
            placement: route.placement.unwrap() as u64,
        })
        .collect();
    let payload = kit::payload(&retired, supplement);
    import(&mut cluster, leader, &retired, &payload).await;
    mark("import committed");
    let applied = raft_kit::durable_position(cluster.host(leader));
    for node in cluster.nodes() {
        cluster.wait_applied(node, applied).await;
        assert_fleet_map(&cluster, node, &routes, generation);
    }
    mark("every voter publishes the fleet map with its tree and codes");

    // 3. An owner on the group and a write through the leader and through
    //    a member that does not lead.
    let catalog_dir = kit::TestDir::new("migration-catalog");
    let catalog = bridge_active(&mut cluster, leader, &catalog_dir).await;
    let follower = *cluster.nodes().iter().find(|n| **n != leader).unwrap();
    let receipt = write_through(&cluster, leader, &catalog, b"doc-leader", b"op-leader").await;
    assert_eq!(receipt.version, 1);
    let receipt = write_through(
        &cluster,
        follower,
        &catalog,
        b"doc-follower",
        b"op-follower",
    )
    .await;
    assert_eq!(receipt.accepted_sequence, 3);
    mark("writes admitted on the leader and on a follower under a forwarded lease");

    // 4. The leader is lost with a write in flight: the write is parked
    //    inside its transaction, the leader is cut off, the survivors elect,
    //    the write commits inside its lease, and after healing its exact
    //    retry replays through the old leader under a forwarded lease.
    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    let catalog = Arc::new(catalog);
    catalog.arm_precommit_pause(Duration::from_millis(300));
    let inflight = catalog_write(b"doc-inflight", b"op-inflight");
    let parked = {
        let host = Arc::new(cluster.take(leader));
        let catalog = Arc::clone(&catalog);
        let request = inflight.clone();
        let task = {
            let host = Arc::clone(&host);
            tokio::spawn(async move {
                host.with_admission("alice", |admission| catalog.accept(admission, &request))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        host.isolate(others.iter().copied());
        for other in &others {
            cluster.host(*other).isolate([leader]);
        }
        (host, task)
    };
    let (old_leader, task) = parked;
    let accepted = task.await.unwrap().unwrap();
    let inflight_receipt = match accepted {
        Acceptance::Accepted(receipt) => receipt,
        Acceptance::Unconfirmed(receipt) => panic!("committed inside its lease: {receipt:?}"),
    };
    assert_eq!(inflight_receipt.accepted_sequence, 4);
    let successor = cluster.leader_other_than(leader).await;
    mark(&format!("leader {leader} cut off with a write in flight; the write committed inside its lease; successor {successor}"));
    old_leader.heal();
    for other in &others {
        cluster.host(*other).heal();
    }
    let position = raft_kit::durable_position(cluster.host(successor));
    old_leader
        .wait(Some(Duration::from_secs(30)))
        .applied_index_at_least(Some(position), "healed")
        .await
        .unwrap();
    // Applied catch-up alone does not mean the healed member knows the
    // leader yet; the replay below forwards to the successor, so wait
    // for that awareness instead of racing the successor's heartbeat.
    // Waiting for the successor specifically (not any leader) keeps the
    // forwarded-lease property the replay asserts.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if old_leader.believed_leader() == Some(successor) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the healed old leader never learned the successor"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let replay = old_leader
        .with_admission("alice", |admission| catalog.accept(admission, &inflight))
        .await
        .unwrap();
    assert_eq!(
        replay,
        Acceptance::Accepted(pipestream_search::pb::DocumentWriteReceipt {
            replayed: true,
            ..inflight_receipt
        })
    );
    let old_leader = Arc::try_unwrap(old_leader)
        .ok()
        .expect("the parked task released the host");
    cluster.insert(leader, old_leader);
    mark("healed: the in-flight write replays through the old leader under a forwarded lease");
    let _ = lease;

    // 5. A member is lost and replaced from a prepared directory while the
    //    leader's published image has changed bytes: the first seed is
    //    rejected by name, the leader rebuilds, the second seed installs,
    //    and the member is a voter again.
    let lost = *others.iter().find(|n| **n != successor).unwrap();
    cluster.host(successor).remove_member(lost).await.unwrap();
    let lost_host = cluster.take(lost);
    lost_host.shutdown().await.unwrap();
    mark(&format!("member {lost} removed and its process gone"));
    let successor_dir = cluster.member_dir(successor);
    let before = cluster
        .host(successor)
        .last_snapshot_build()
        .map(|b| b.generation);
    cluster.host(successor).trigger_snapshot().await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if cluster
                .host(successor)
                .last_snapshot_build()
                .map(|b| b.generation)
                > before
                && cluster.host(successor).verify_published_image().is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the successor published an image");
    let (image, _) = raft_kit::published_pair(&successor_dir).unwrap();
    let published = raft_kit::published_generation(&successor_dir).unwrap();
    let image_path = raft_kit::generation_dir(&successor_dir, published).join("image.redb");
    let mut bytes = std::fs::read(&image_path).unwrap();
    assert_eq!(bytes, image);
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&image_path, &bytes).unwrap();
    let fresh = kit::TestDir::new("migration-replacement");
    RaftHost::prepare_member(
        fresh.path(),
        &cluster.group,
        lost,
        &policy(),
        &kit::limits(),
    )
    .unwrap();
    let replacement = RaftHost::start_member(
        fresh.path(),
        &cluster.group,
        lost,
        &cluster.config,
        raft_kit::transport(lost, &cluster.directory, "127.0.0.1:0".parse().unwrap()),
    )
    .await
    .unwrap();
    let addr = replacement.advertised_addr().unwrap().to_string();
    let rejected = tokio::time::timeout(
        Duration::from_secs(20),
        cluster.host(successor).add_learner(lost, &addr),
    )
    .await
    .expect("the leader answers inside 20 s")
    .err()
    .expect("an image with changed bytes was installed");
    assert_eq!(rejected.code(), Code::DataLoss, "{rejected}");
    mark("the changed image is rejected at the replacement by name");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if cluster
                .host(successor)
                .last_snapshot_build()
                .map(|b| b.generation)
                > Some(published)
                && cluster.host(successor).verify_published_image().is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the leader rebuilt its image");
    tokio::time::timeout(
        Duration::from_secs(30),
        cluster.host(successor).add_learner(lost, &addr),
    )
    .await
    .expect("the join answers inside 30 s")
    .expect("the replacement is seeded from the fresh image");
    cluster
        .host(successor)
        .promote(BTreeSet::from([lost]))
        .await
        .unwrap();
    cluster.insert(lost, replacement);
    cluster
        .host(successor)
        .wait(Some(Duration::from_secs(30)))
        .voter_ids(
            cluster.nodes().into_iter().collect::<Vec<_>>(),
            "three voters again",
        )
        .await
        .unwrap();
    let applied = raft_kit::durable_position(cluster.host(successor));
    cluster.wait_applied(lost, applied).await;
    assert_fleet_map(&cluster, lost, &routes, generation);
    mark(&format!("member {lost} replaced from a prepared directory, seeded from the rebuilt image, a voter again, publishing the fleet map"));

    // 6. Cold restart: every member down, every member up on its directory,
    //    a leader elected, the map unchanged, a write admitted anywhere.
    let mut addrs = std::collections::BTreeMap::new();
    let mut dirs = std::collections::BTreeMap::new();
    for node in cluster.nodes() {
        addrs.insert(node, cluster.host(node).listen_addr().unwrap());
        dirs.insert(
            node,
            if node == lost {
                fresh.path().to_path_buf()
            } else {
                cluster.member_dir(node)
            },
        );
    }
    // The activated handle keeps the old leader's store open; it is
    // dropped before the members go down and recovered afterwards through
    // the function the binary uses at start.
    let catalog_path = catalog_dir.path().join("catalog.redb");
    drop(catalog);
    for node in cluster.nodes() {
        cluster.take(node).shutdown().await.unwrap();
    }
    mark("every member down");
    for (node, addr) in &addrs {
        let host = RaftHost::start_member(
            &dirs[node],
            &cluster.group,
            *node,
            &cluster.config,
            raft_kit::transport(*node, &cluster.directory, *addr),
        )
        .await
        .unwrap();
        cluster.insert(*node, host);
    }
    let leader = cluster.leader().await;
    mark(&format!("every member up from cold, leader {leader}"));
    let applied = raft_kit::durable_position(cluster.host(leader));
    for node in cluster.nodes() {
        cluster.wait_applied(node, applied).await;
        assert_fleet_map(&cluster, node, &routes, generation);
    }
    let any = *cluster.nodes().iter().find(|n| **n != leader).unwrap();
    let recovered = recover_catalogs(
        cluster.host(any),
        "alice",
        &[RaftManagedCatalog {
            collection: kit::COLLECTION.into(),
            path: catalog_path.clone(),
        }],
    )
    .unwrap();
    let catalog = Arc::clone(&recovered[0]);
    let receipt = write_through(&cluster, any, &catalog, b"doc-after-cold", b"op-after-cold").await;
    assert_eq!(receipt.accepted_sequence, 5);
    mark(&format!(
        "a write admitted through member {any} after the cold restart"
    ));

    // 7. The retirement record is recoverable evidence: with every holder
    //    dropped, exact Admin recovery returns the same checkpoint.
    drop(retired);
    let store = cluster.host(leader).store().unwrap();
    let recovered = store
        .recover_legacy_retirement("alice", &request, &legacy_dir.legacy())
        .unwrap();
    assert_eq!(
        pipestream_search::sha256::digest(recovered.checkpoint_bytes()),
        checkpoint_digest
    );
    drop(recovered);
    drop(store);
    mark("the retirement record recovered with the same checkpoint digest");

    cluster.shutdown().await;
    mark("done");
}
