//! Integration tests for the operability round: pooled channels, health
//! reporting, replica failover, hedged retries, per-shard deadlines, and
//! the floor delta gate. Everything is seeded and exact — the tests
//! assert bitwise equality against the monolithic reference wherever a
//! search result is involved.

mod common;

use common::{monolithic_topk, Cluster, DIM};
use tokio::net::TcpListener;
use tonic::Request;
use turbovec_search::coordinator::{CoordinatorServiceImpl, FanoutLimits};
use turbovec_search::harness::unit_vectors;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService;
use turbovec_search::pb::{ClusterHealthRequest, HealthRequest};

const K: u32 = 25;

fn fanout_hits(result: &turbovec_search::coordinator::FanoutResult) -> Vec<(u64, u32)> {
    result
        .hits
        .iter()
        .map(|h| (h.vector_id, h.score.to_bits()))
        .collect()
}

/// A TCP listener that accepts connections but never speaks HTTP/2: the
/// canonical hanging node. Connections established against it stall until
/// something above them times out.
async fn hanging_addr() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            // Hold the socket open, say nothing.
            std::mem::forget(socket);
        }
    });
    (addr, handle)
}

#[tokio::test]
async fn health_reports_shard_shape() {
    let cluster = Cluster::start(3_000, 64, true).await;
    let mut client = NodeServiceClient::connect(cluster.node_addrs[0].clone())
        .await
        .unwrap();
    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert!(health.num_vectors > 0);
    assert_eq!(health.dim, DIM as u32);
    assert_eq!(health.bit_width, 4);
    assert!(!health.ingest_active);
    assert!(!health.bm25_building);
    assert_eq!(health.bm25_docs, 0);
    cluster.shutdown().await;
}

#[tokio::test]
async fn cluster_health_reports_reachable_and_unreachable_targets() {
    let cluster = Cluster::start(3_000, 64, true).await;
    // One live primary, one dead primary (bound then dropped: refused),
    // and a live replica for the dead one.
    let dead = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        format!("http://{}", l.local_addr().unwrap())
    };
    let coordinator = CoordinatorServiceImpl::new(vec![cluster.node_addrs[0].clone(), dead])
        .with_replicas(vec![None, Some(cluster.node_addrs[1].clone())]);
    let report = coordinator
        .cluster_health(Request::new(ClusterHealthRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.targets.len(), 3);
    let primary_live = &report.targets[0];
    assert!(primary_live.reachable && !primary_live.is_replica);
    assert!(primary_live.health.as_ref().unwrap().num_vectors > 0);
    let primary_dead = &report.targets[1];
    assert!(!primary_dead.reachable);
    assert!(!primary_dead.error.is_empty());
    let replica = &report.targets[2];
    assert!(replica.reachable && replica.is_replica);
    assert_eq!(replica.shard, 1);
    cluster.shutdown().await;
}

#[tokio::test]
async fn replica_failover_returns_identical_results() {
    let cluster = Cluster::start(6_000, 64, true).await;
    let query = unit_vectors(1, DIM, 0x0FA1_C0DE);
    let expected = monolithic_topk(&cluster.monolithic, &query, K as usize);

    // Shard 1's primary refuses connections; its replica is the real node.
    let dead = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        format!("http://{}", l.local_addr().unwrap())
    };
    let mut addrs = cluster.node_addrs.clone();
    let real = std::mem::replace(&mut addrs[1], dead);
    let coordinator = CoordinatorServiceImpl::new(addrs)
        .with_replicas(vec![None, Some(real), None]);

    let result = coordinator
        .fanout_search("failover-test", &query, K, false)
        .await
        .expect("failover to the replica must succeed");
    assert_eq!(fanout_hits(&result), expected);
    cluster.shutdown().await;
}

#[tokio::test]
async fn hedged_query_returns_identical_results() {
    let cluster = Cluster::start(6_000, 64, true).await;
    let query = unit_vectors(1, DIM, 0x4ED6_E001);
    let expected = monolithic_topk(&cluster.monolithic, &query, K as usize);

    // Every shard hedges to itself almost immediately: the race path runs
    // on every shard and first-success-wins must not duplicate or drop.
    let replicas = cluster.node_addrs.iter().cloned().map(Some).collect();
    let coordinator = CoordinatorServiceImpl::new(cluster.node_addrs.clone())
        .with_replicas(replicas)
        .with_limits(FanoutLimits {
            shard_deadline: Some(std::time::Duration::from_secs(30)),
            hedge_delay: Some(std::time::Duration::from_millis(1)),
        });

    for round in 0..5 {
        let result = coordinator
            .fanout_search(&format!("hedge-{round}"), &query, K, false)
            .await
            .expect("hedged fan-out must succeed");
        assert_eq!(fanout_hits(&result), expected, "round {round}");
    }
    cluster.shutdown().await;
}

#[tokio::test]
async fn shard_deadline_fires_on_a_hanging_node() {
    let cluster = Cluster::start(3_000, 64, true).await;
    let (hanging, guard) = hanging_addr().await;
    let mut addrs = cluster.node_addrs.clone();
    addrs[2] = hanging;
    let coordinator = CoordinatorServiceImpl::new(addrs).with_limits(FanoutLimits {
        shard_deadline: Some(std::time::Duration::from_millis(300)),
        hedge_delay: None,
    });

    let query = unit_vectors(1, DIM, 0xDEAD_11E5);
    let err = coordinator
        .fanout_search("deadline-test", &query, K, false)
        .await
        .expect_err("a hanging shard must trip the deadline");
    assert!(
        err.message().contains("deadline"),
        "unexpected error: {err}"
    );
    guard.abort();
    cluster.shutdown().await;
}

#[tokio::test]
async fn pooled_channel_reconnects_after_node_restart() {
    use tonic::transport::Server;
    use turbovec_search::harness::{
        build_monolithic, build_shards, fit_calibration, nodelay_incoming,
    };
    use turbovec_search::node::{NodeConfig, NodeServiceImpl};
    use turbovec_search::MAX_MESSAGE_BYTES;

    let n = 3_000;
    let corpus = unit_vectors(n, DIM, 0x5EED_CA11);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus[..2_000 * DIM]);
    let monolithic = build_monolithic(&corpus, DIM, 4, &shift, &scale);
    let build_node = || {
        let mut shards = build_shards(&corpus, DIM, 4, 1, &shift, &scale);
        NodeServiceImpl::new(Some(shards.remove(0).index), NodeConfig::default())
    };

    // First incarnation on a fixed ephemeral port.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let first = tokio::spawn(
        Server::builder()
            .add_service(NodeServiceImpl::into_server(build_node(), MAX_MESSAGE_BYTES))
            .serve_with_incoming(nodelay_incoming(listener)),
    );

    let coordinator = CoordinatorServiceImpl::new(vec![format!("http://{addr}")]);
    let query = unit_vectors(1, DIM, 0x2EC0_44EC);
    let expected = monolithic_topk(&monolithic, &query, K as usize);
    let before = coordinator
        .fanout_search("reconnect-before", &query, K, false)
        .await
        .unwrap();
    assert_eq!(fanout_hits(&before), expected);

    // Kill the node, wait for the port to actually close, then restart a
    // fresh incarnation on the SAME port. The coordinator keeps its pooled
    // channel throughout — the query after the restart must succeed
    // without any reconstruction on the coordinator side.
    first.abort();
    let _ = first.await;
    let listener = loop {
        match TcpListener::bind(addr).await {
            Ok(l) => break l,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
        }
    };
    let second = tokio::spawn(
        Server::builder()
            .add_service(NodeServiceImpl::into_server(build_node(), MAX_MESSAGE_BYTES))
            .serve_with_incoming(nodelay_incoming(listener)),
    );

    // The first attempt after a restart may catch the channel mid-reset;
    // the channel must recover on its own within a couple of tries.
    let mut after = None;
    for _ in 0..20 {
        match coordinator
            .fanout_search("reconnect-after", &query, K, false)
            .await
        {
            Ok(result) => {
                after = Some(result);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    let after = after.expect("pooled channel never recovered after node restart");
    assert_eq!(fanout_hits(&after), expected);
    second.abort();
}

#[tokio::test]
async fn floor_delta_gate_never_changes_results() {
    // A delta so large no floor ever clears the gate: pruning hints stop
    // flowing entirely, results must not move.
    use turbovec_search::harness::{
        build_monolithic, build_shards, fit_calibration, start_node,
    };
    use turbovec_search::node::NodeConfig;

    let n = 6_000;
    let corpus = unit_vectors(n, DIM, 0x5EED_CA11);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus[..2_000 * DIM]);
    let shards = build_shards(&corpus, DIM, 4, 3, &shift, &scale);
    let monolithic = build_monolithic(&corpus, DIM, 4, &shift, &scale);

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in shards {
        let (addr, handle) = start_node(
            shard.index,
            NodeConfig {
                slot_offset: shard.slot_offset,
                chunk_blocks: 64,
                share_floors: true,
                floor_delta: 1_000.0,
                ..Default::default()
            },
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }

    let coordinator = CoordinatorServiceImpl::new(addrs);
    let query = unit_vectors(1, DIM, 0xDE17_A001);
    let expected = monolithic_topk(&monolithic, &query, K as usize);
    let result = coordinator
        .fanout_search("delta-test", &query, K, false)
        .await
        .unwrap();
    assert_eq!(fanout_hits(&result), expected);
    for handle in handles {
        handle.abort();
    }
}
