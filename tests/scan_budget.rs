//! Bandwidth as the budget (`docs/bandwidth-budget.md`): a node's
//! observed scan rate travels on its lease renewal, and the control
//! plane's `PlanBalance` dry run plans whole-shard moves in the rate's
//! units, excluding a device node by declaration.

mod common;

use std::path::{Path, PathBuf};

use common::{fit_calibration, unit_vectors, BIT_WIDTH, DIM};
use pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND;
use pipestream_search::chunked::encoded_row_bytes;
use pipestream_search::control_plane::{ClusterControlService, ControlPolicy, DurableControlPlane};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness::serve_node;
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl, SCAN_WINDOW_MIN_SAMPLES};
use pipestream_search::node_agent::{NodeAgent, NodeAgentConfig, ServedShard};
use pipestream_search::pb::cluster_control_client::ClusterControlClient;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddVectorsRequest, ClusterPlan, GetClusterPlanRequest, NodeCapacity, NodeResidency,
    PlanBalanceRequest, RegisterNodeRequest, SearchRequest, SetCalibrationRequest,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const ROWS: usize = 3_000;

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("scan_budget_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn shard_config(index_path: PathBuf) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        layout: Layout::SingleImage,
        wal: true,
        ..Default::default()
    }
}

fn agent_config(
    node_id: &str,
    data_dir: PathBuf,
    control_addr: &str,
    node_addr: &str,
) -> NodeAgentConfig {
    NodeAgentConfig {
        node_id: node_id.to_string(),
        control_addr: control_addr.to_string(),
        collection: String::new(),
        failure_domain: format!("rack-{node_id}"),
        data_dir,
        node_addr: node_addr.to_string(),
        advertise_host: "127.0.0.1".to_string(),
        replica_listen: "127.0.0.1:0".parse().unwrap(),
        lease_ms: 60_000,
        report_ms: 10_000,
        reconcile_ms: 10_000,
        lag_bound: 8,
        scan_parallel: 2,
        template: shard_config(PathBuf::new()),
        phrase_index: None,
        allow_missing_bm25: false,
        tls: None,
        max_message_bytes: pipestream_search::MAX_MESSAGE_BYTES,
    }
}

async fn client(addr: &str) -> NodeServiceClient<tonic::transport::Channel> {
    NodeServiceClient::connect(addr.to_string()).await.unwrap()
}

async fn control_client(addr: &str) -> ClusterControlClient<tonic::transport::Channel> {
    ClusterControlClient::connect(addr.to_string())
        .await
        .unwrap()
}

async fn serve_control(control: ClusterControlService) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(control.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    addr
}

/// A calibrated node with ROWS vectors, served, plus its corpus.
async fn seeded_node(dir: &Path) -> (NodeServiceImpl, String, Vec<f32>) {
    let node = NodeServiceImpl::open(shard_config(dir.join("shard")), None, false).unwrap();
    let (addr, _handle) = serve_node(node.clone()).await;
    let corpus = unit_vectors(ROWS, DIM, 0xB0D6_0001);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..2_048 * DIM]);
    let mut c = client(&addr).await;
    c.set_calibration(SetCalibrationRequest {
        dim: DIM as u32,
        bit_width: BIT_WIDTH as u32,
        shift,
        scale,
    })
    .await
    .unwrap();
    let (tx, rx) = mpsc::channel(8);
    let batch = corpus.clone();
    tokio::spawn(async move {
        let _ = tx
            .send(AddVectorsRequest {
                vectors: batch,
                dim: DIM as u32,
            })
            .await;
    });
    let added = c
        .add_vectors(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
        .added;
    assert_eq!(added as usize, ROWS);
    (node, addr, corpus)
}

/// The coordinator, the plane, and the control listener over one node,
/// with the plane bootstrapped from the coordinator's topology.
async fn cluster_over(addr: &str, dir: &Path) -> (CoordinatorServiceImpl, String) {
    let coordinator = CoordinatorServiceImpl::new(vec![addr.to_string()])
        .with_topology_generation(1)
        .with_hot_topology(vec![Some((0, u64::MAX))])
        .unwrap();
    let plane = DurableControlPlane::open(
        dir.join("control.json"),
        ControlPolicy {
            lease_ms: 60_000,
            replication_factor: 1,
            split_rows: u64::MAX,
            merge_rows: 0,
            compact_segments: u32::MAX,
            compact_tombstone_ppm: 1_000_000,
            history_limit: 8,
        },
    )
    .unwrap();
    plane
        .bootstrap_topology(1, &coordinator.current_topology_routes())
        .unwrap();
    let control_addr =
        serve_control(ClusterControlService::new(plane).with_coordinator(coordinator.clone()))
            .await;
    (coordinator, control_addr)
}

/// Enough searches through the coordinator to fill the node's window
/// past its minimum.
async fn scan_a_few_times(coordinator: &CoordinatorServiceImpl, corpus: &[f32]) {
    for i in 0..(SCAN_WINDOW_MIN_SAMPLES + 2) {
        let vector = corpus[i * DIM..(i + 1) * DIM].to_vec();
        let hits = SearchService::search(
            coordinator,
            Request::new(SearchRequest {
                request_id: format!("scan-{i}"),
                k: 5,
                vector,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(hits.hits.len(), 5);
    }
}

fn capacity_of<'a>(plan: &'a ClusterPlan, node_id: &str) -> &'a NodeCapacity {
    plan.nodes
        .iter()
        .find(|n| n.node_id == node_id)
        .unwrap_or_else(|| panic!("node {node_id} in the plan"))
        .capacity
        .as_ref()
        .expect("capacity")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_observed_rate_reaches_the_authority_on_renewal() {
    let dir = tempdir("renewal");
    let (node, addr, corpus) = seeded_node(&dir).await;
    let (coordinator, control_addr) = cluster_over(&addr, &dir).await;
    let agent = NodeAgent::new(
        agent_config("a", dir.join("a-data"), &control_addr, &addr),
        vec![ServedShard::configured(
            "s0",
            node.clone(),
            addr.clone(),
            Some((0, u64::MAX)),
        )],
    );
    agent.register().await.unwrap();

    let (bytes_before, nanos_before) = pipestream_search::metrics::scan_pass_totals();
    scan_a_few_times(&coordinator, &corpus).await;
    let (bytes_after, nanos_after) = pipestream_search::metrics::scan_pass_totals();
    let row_bytes = encoded_row_bytes(DIM, BIT_WIDTH);
    assert_eq!(row_bytes, (DIM / 2 + 4) as u64);
    // Each unfiltered search streamed the whole index once; other tests
    // in this process may add to the totals, never subtract.
    assert!(
        bytes_after - bytes_before
            >= (SCAN_WINDOW_MIN_SAMPLES + 2) as u64 * ROWS as u64 * row_bytes,
        "scan bytes {bytes_before} -> {bytes_after}"
    );
    assert!(nanos_after > nanos_before, "active scan time was observed");

    let rate = pipestream_search::node::scan_rate();
    assert!(rate.samples as usize >= SCAN_WINDOW_MIN_SAMPLES, "{rate:?}");
    assert!(rate.bytes_per_second > 0, "{rate:?}");
    assert!(rate.observed_unix_ms > 0, "{rate:?}");
    assert_eq!(rate.window_ms, pipestream_search::node::SCAN_WINDOW_MS);

    // The renewal carries the same figures, and the plan shows them.
    agent.renew().await.unwrap();
    let plan = control_client(&control_addr)
        .await
        .get_cluster_plan(GetClusterPlanRequest {
            collection: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let capacity = capacity_of(&plan, "a");
    assert_eq!(capacity.scan_bytes_per_second, rate.bytes_per_second);
    assert_eq!(capacity.scan_rate_observed_unix_ms, rate.observed_unix_ms);
    assert_eq!(capacity.scan_rate_samples, rate.samples);
    assert_eq!(capacity.scan_rate_window_ms, rate.window_ms);
    assert_eq!(capacity.residency, NodeResidency::Server as i32);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_balance_dry_run_plans_in_row_bytes_and_excludes_the_device() {
    let dir = tempdir("balance");
    let (node, addr, corpus) = seeded_node(&dir).await;
    let (coordinator, control_addr) = cluster_over(&addr, &dir).await;
    let agent = NodeAgent::new(
        agent_config("a", dir.join("a-data"), &control_addr, &addr),
        vec![ServedShard::configured(
            "s0",
            node.clone(),
            addr.clone(),
            Some((0, u64::MAX)),
        )],
    );
    agent.register().await.unwrap();
    scan_a_few_times(&coordinator, &corpus).await;
    agent.renew().await.unwrap();
    assert!(agent.report_shard("s0").await.unwrap(), "s0 reported");
    let rate = pipestream_search::node::scan_rate();
    assert!(rate.bytes_per_second > 0);

    let mut control = control_client(&control_addr).await;
    // A second server, far faster than a and empty: the plan's only
    // legal destination.
    let b = control
        .register_node(RegisterNodeRequest {
            collection: String::new(),
            node_id: "b".into(),
            addr: "10.0.0.2:1".into(),
            capacity: Some(NodeCapacity {
                disk_bytes: 1 << 40,
                failure_domain: "rack-b".into(),
                scan_bytes_per_second: rate.bytes_per_second.saturating_mul(1_000).max(1 << 40),
                scan_rate_observed_unix_ms: rate.observed_unix_ms,
                scan_rate_samples: 8,
                scan_rate_window_ms: rate.window_ms,
                residency: NodeResidency::Server as i32,
                ..Default::default()
            }),
            lease_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(b.node_id, "b");
    // A phone: the fastest node by far, and off limits by declaration.
    control
        .register_node(RegisterNodeRequest {
            collection: String::new(),
            node_id: "phone".into(),
            addr: "10.9.9.9:1".into(),
            capacity: Some(NodeCapacity {
                disk_bytes: 1 << 36,
                failure_domain: "pocket".into(),
                scan_bytes_per_second: u64::MAX / 4,
                scan_rate_observed_unix_ms: rate.observed_unix_ms,
                scan_rate_samples: 8,
                scan_rate_window_ms: rate.window_ms,
                residency: NodeResidency::Device as i32,
                ..Default::default()
            }),
            lease_ms: 60_000,
        })
        .await
        .unwrap()
        .into_inner();
    // The phone reports no shard here: a second primary over the whole
    // hash space would not tile the topology. Its shards' exclusion is
    // proven on the planner directly; the wire test proves the node is
    // excluded by declaration and never a destination.

    let plan = control
        .plan_balance(PlanBalanceRequest {
            collection: String::new(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let row_bytes = encoded_row_bytes(DIM, BIT_WIDTH);
    assert_eq!(plan.topology_generation, 1);
    assert!(plan.control_revision > 0);
    assert_eq!(plan.min_gain, 0.10);
    assert_eq!(plan.max_moves, 8);
    let a_load = plan.loads.iter().find(|l| l.node_id == "a").unwrap();
    assert_eq!(
        a_load.bytes,
        ROWS as u64 * row_bytes,
        "rows times encoded row bytes"
    );
    assert_eq!(a_load.shards, vec![0], "s0 is route 0 of the topology");
    // The renewal took the window's rate at that moment; the sibling
    // test in this process keeps scanning, so compare with the stored
    // capacity rather than a later reading of the window.
    let stored = control
        .get_cluster_plan(GetClusterPlanRequest {
            collection: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        a_load.scan_bytes_per_second,
        capacity_of(&stored, "a").scan_bytes_per_second
    );
    assert!(a_load.scan_bytes_per_second > 0);
    assert!(a_load.seconds > 0.0);
    let phone_load = plan.loads.iter().find(|l| l.node_id == "phone").unwrap();
    assert_eq!(phone_load.bytes, 0);
    assert_eq!(phone_load.seconds, 0.0, "an excluded node gets no estimate");
    assert_eq!(phone_load.residency, NodeResidency::Device as i32);
    assert!(
        plan.excluded
            .iter()
            .any(|e| e.node_id == "phone" && e.reason == "device"),
        "{:?}",
        plan.excluded
    );
    assert!(plan
        .excluded
        .iter()
        .all(|e| e.node_id != "a" && e.node_id != "b"));
    // The single move: s0 from a to b, whole shard, in row-byte units.
    assert_eq!(plan.moves.len(), 1, "{:?}", plan.moves);
    let m = &plan.moves[0];
    assert_eq!(
        (m.shard, m.from_node.as_str(), m.to_node.as_str()),
        (0, "a", "b")
    );
    assert_eq!(m.bytes, ROWS as u64 * row_bytes);
    assert_eq!(m.leaf, "", "no placement tree");
    assert!(m.seconds_after < plan.seconds_before);
    assert_eq!(plan.seconds_after, m.seconds_after);
    for m in &plan.moves {
        assert_ne!(m.from_node, "phone");
        assert_ne!(m.to_node, "phone");
    }

    // The thresholds are validated by name over the wire.
    let err = control
        .plan_balance(PlanBalanceRequest {
            collection: String::new(),
            min_gain: 1.5,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("min_gain"), "{err}");
    let _ = std::fs::remove_dir_all(&dir);
}
