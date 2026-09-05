//! `SPLIT_SHARD` end to end (`docs/cluster-control.md`, "Shard split"): the
//! plane plans a split of an over-full primary; the node's worker builds
//! two children from the source's WAL, places them, tails the source
//! into them by stable key, fences the source, completes with the
//! children as primaries, and retires the source; the plane publishes a
//! topology that tiles the range with the children, and every query
//! answers with the same scores through the coordinator. A restart of
//! the agent keeps the source retired and re-serves the children.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::{fit_calibration, unit_vectors, BIT_WIDTH, DIM};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::control_plane::{ClusterControlService, ControlPolicy, DurableControlPlane};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness::serve_node;
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl};
use pipestream_search::node_agent::{NodeAgent, NodeAgentConfig, ServedShard};
use pipestream_search::pb::cluster_control_client::ClusterControlClient;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, Bm25SearchRequest, ClusterPlan, FacetValue,
    FlushRequest, HealthRequest, PlacementActionKind, ReconcileClusterRequest, SearchRequest,
    SetCalibrationRequest, ShardReplicaRole,
};
use tokio::net::TcpListener;
use tonic::Request;

const DOCS: usize = 40;
const COURTS: [&str; 4] = ["scotus", "ca9", "ca5", "nysd"];
const WORDS: [&str; 8] = [
    "court", "appeal", "claim", "denied", "brief", "steps", "reporter", "motion",
];

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("split_shard_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn shard_config(index_path: PathBuf) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        facet_fields: vec!["court".to_string()],
        layout: Layout::SingleImage,
        wal: true,
        ..Default::default()
    }
}

fn agent_config(dir: &std::path::Path, control_addr: &str, node_addr: &str) -> NodeAgentConfig {
    NodeAgentConfig {
        node_id: "a".to_string(),
        control_addr: control_addr.to_string(),
        collection: String::new(),
        failure_domain: "rack-a".to_string(),
        data_dir: dir.join("a-data"),
        node_addr: node_addr.to_string(),
        advertise_host: "127.0.0.1".to_string(),
        replica_listen: "127.0.0.1:0".parse().unwrap(),
        lease_ms: 60_000,
        report_ms: 10_000,
        reconcile_ms: 10_000,
        lag_bound: 0,
        scan_parallel: 2,
        template: shard_config(PathBuf::new()),
        phrase_index: None,
        allow_missing_bm25: false,
        tls: None,
        max_message_bytes: pipestream_search::MAX_MESSAGE_BYTES,
    }
}

fn doc_text(i: usize) -> String {
    (0..(3 + i % 5))
        .map(|j| WORDS[(i * 7 + j * 3) % WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

async fn client(addr: &str) -> NodeServiceClient<tonic::transport::Channel> {
    NodeServiceClient::connect(addr.to_string()).await.unwrap()
}

/// One document and its vector, each as a stream carrying the row's
/// stable routing key (the form replication and the live tail use).
async fn ingest_keyed(addr: &str, i: usize) -> Result<(), tonic::Status> {
    let key = format!("case-{i}");
    let mut c = client(addr).await;
    let mut request = Request::new(tokio_stream::iter([AddDocumentsRequest {
        text: doc_text(i),
        analysis: Some(body_spec()),
        facets: vec![FacetValue {
            field: "court".into(),
            value: COURTS[i % COURTS.len()].into(),
        }],
        ..Default::default()
    }]));
    request.metadata_mut().insert_bin(
        "x-protomolt-stable-key-bin",
        tonic::metadata::MetadataValue::from_bytes(key.as_bytes()),
    );
    c.add_documents(request).await?;
    let corpus = unit_vectors(i + 1, DIM, 0x5EED_CA11);
    let mut request = Request::new(tokio_stream::iter([AddVectorsRequest {
        vectors: corpus[i * DIM..(i + 1) * DIM].to_vec(),
        dim: DIM as u32,
    }]));
    request.metadata_mut().insert_bin(
        "x-protomolt-stable-key-bin",
        tonic::metadata::MetadataValue::from_bytes(key.as_bytes()),
    );
    c.add_vectors(request).await?;
    Ok(())
}

/// Scores per probe (ids change across a split; scores and facet
/// counts do not).
async fn signature(c: &CoordinatorServiceImpl) -> Vec<String> {
    let base = |text: &str| Bm25SearchRequest {
        text: text.to_string(),
        k: 30,
        analysis: Some(body_spec()),
        ..Default::default()
    };
    let mut out = Vec::new();
    for probe in [
        base("court"),
        base("court appeal"),
        Bm25SearchRequest {
            facet_fields: vec!["court".into()],
            ..base("claim denied")
        },
        Bm25SearchRequest {
            filter: "court == \"ca9\"".into(),
            ..base("court")
        },
    ] {
        let resp = SearchService::bm25_search(c, Request::new(probe))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.hits.is_empty());
        let mut scores: Vec<u32> = resp.hits.iter().map(|h| h.score.to_bits()).collect();
        scores.sort_unstable();
        out.push(format!(
            "{:?}",
            (scores, resp.kth_best.to_bits(), resp.facets)
        ));
    }
    let queries = unit_vectors(3, DIM, 0x0E0E_0001);
    for q in 0..3 {
        let resp = SearchService::search(
            c,
            Request::new(SearchRequest {
                k: 10,
                vector: queries[q * DIM..(q + 1) * DIM].to_vec(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(resp.hits.len(), 10);
        let mut scores: Vec<u32> = resp.hits.iter().map(|h| h.score.to_bits()).collect();
        scores.sort_unstable();
        out.push(format!("{scores:?}"));
    }
    out
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

async fn reconcile(addr: &str) -> ClusterPlan {
    ClusterControlClient::connect(addr.to_string())
        .await
        .unwrap()
        .reconcile_cluster(ReconcileClusterRequest {
            dry_run: false,
            collection: String::new(),
        })
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_primary_splits_online_into_two_children_that_tile_its_range() {
    let dir = tempdir("split");
    let node_a = NodeServiceImpl::open(shard_config(dir.join("a/shard")), None, false).unwrap();
    let (addr_a, _handle_a) = serve_node(node_a.clone()).await;
    let corpus = unit_vectors(2_000, DIM, 0xCA11_0001);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus);
    client(&addr_a)
        .await
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
    for i in 0..DOCS {
        ingest_keyed(&addr_a, i).await.unwrap();
    }
    client(&addr_a).await.flush(FlushRequest {}).await.unwrap();

    let coordinator = CoordinatorServiceImpl::new(vec![addr_a.clone()])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(1)
        .with_hot_topology(vec![Some((0, u64::MAX))])
        .unwrap();
    let plane = DurableControlPlane::open(
        dir.join("control.json"),
        ControlPolicy {
            lease_ms: 60_000,
            replication_factor: 1,
            // Above the source's 40 rows' worth of half: the source splits,
            // its children (about 22 rows each) do not qualify again.
            split_rows: 35,
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
    let agent = NodeAgent::new(
        agent_config(&dir, &control_addr, &addr_a),
        vec![ServedShard::configured(
            "s0",
            node_a.clone(),
            addr_a.clone(),
            Some((0, u64::MAX)),
        )],
    );
    agent.register().await.unwrap();
    assert_eq!(agent.report_all().await.unwrap(), 1);
    let plan = reconcile(&control_addr).await;
    let split = plan
        .actions
        .iter()
        .find(|a| a.kind == PlacementActionKind::SplitShard as i32)
        .cloned()
        .expect("the plane plans a split of the over-full primary");
    assert_eq!(
        (split.shard_id.as_str(), split.target_node_id.as_str()),
        ("s0", "a")
    );
    assert_eq!((split.hash_lo, split.hash_hi), (0, u64::MAX));

    // Rows that land after the plan (and after the flush the baseline is
    // cut at) must reach the children through the live tail; rows after
    // the fence are refused by name. The hook runs after the fence and
    // the final drain, right before completion.
    for i in DOCS..DOCS + 5 {
        ingest_keyed(&addr_a, i).await.unwrap();
    }
    let want = signature(&coordinator).await;
    let fenced_refusals = Arc::new(std::sync::atomic::AtomicU64::new(0));
    {
        let (addr, refusals) = (addr_a.clone(), Arc::clone(&fenced_refusals));
        agent.set_before_complete(Some(Arc::new(move || {
            let (addr, refusals) = (addr.clone(), Arc::clone(&refusals));
            Box::pin(async move {
                let error = ingest_keyed(&addr, 999).await.unwrap_err();
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                assert!(error.message().contains("fenced"), "{}", error.message());
                assert!(error.message().contains("s0-0"), "{}", error.message());
                refusals.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            })
        })));
    }
    let started = std::time::Instant::now();
    agent.run_once().await.unwrap();
    eprintln!("split of s0: {} ms", started.elapsed().as_millis());
    assert_eq!(
        agent
            .stats()
            .splits_completed
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        fenced_refusals.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    agent.set_before_complete(None);

    // The plane: the source's record is gone, two primaries tile the
    // range and conserve the rows, the topology moved on.
    let plan = reconcile(&control_addr).await;
    assert!(plan
        .actions
        .iter()
        .all(|a| a.kind != PlacementActionKind::SplitShard as i32 || a.shard_id != "s0"));
    assert!(plan.replicas.iter().all(|r| r.shard_id != "s0"));
    let mut children: Vec<_> = plan
        .replicas
        .iter()
        .filter(|r| r.shard_id.starts_with("s0-"))
        .cloned()
        .collect();
    children.sort_by_key(|r| r.hash_lo);
    assert_eq!(children.len(), 2);
    assert_eq!(children[0].shard_id, "s0-0");
    assert_eq!(children[1].shard_id, "s0-1");
    assert_eq!(children[0].hash_lo, 0);
    assert_eq!(children[0].hash_hi + 1, children[1].hash_lo);
    assert_eq!(children[1].hash_hi, u64::MAX);
    assert!(children
        .iter()
        .all(|r| r.role == ShardReplicaRole::Primary as i32 && r.ready));
    assert_eq!(
        children[0].rows + children[1].rows,
        (DOCS + 5) as u64,
        "every row, the tailed ones included, lives in a child"
    );
    assert!(
        children.iter().all(|r| r.rows > 0),
        "the keys spread over both halves"
    );
    assert_ne!(children[0].slot_offset, children[1].slot_offset);
    assert!(plan.topology_generation > 1);
    let routes = coordinator.current_topology_routes();
    assert_eq!(
        routes.len(),
        2,
        "the coordinator serves the published children"
    );
    assert!(coordinator.current_topology_generation() > 1);

    // The same scores through the coordinator, now from the children.
    assert_eq!(signature(&coordinator).await, want);
    let mut served = agent.shard_ids();
    served.sort();
    assert_eq!(served, vec!["s0-0".to_string(), "s0-1".to_string()]);
    let health = client(&addr_a)
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        health.bm25_docs,
        (DOCS + 5) as u64,
        "the retired source keeps its files"
    );

    // A second pass finds nothing to do.
    agent.run_once().await.unwrap();
    assert_eq!(
        agent
            .stats()
            .splits_completed
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    // A restart: the retired source is not served again, the children
    // come back from their placed records, and the fleet reports two.
    agent.stop();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let agent = NodeAgent::new(
        agent_config(&dir, &control_addr, &addr_a),
        vec![ServedShard::configured(
            "s0",
            node_a.clone(),
            addr_a.clone(),
            Some((0, u64::MAX)),
        )],
    );
    assert!(
        agent.shard_ids().is_empty(),
        "the retired source is filtered"
    );
    let mut opened = agent.open_placed().await.unwrap();
    opened.sort();
    assert_eq!(opened, vec!["s0-0".to_string(), "s0-1".to_string()]);
    agent.register().await.unwrap();
    assert_eq!(agent.report_all().await.unwrap(), 2);
    assert_eq!(signature(&coordinator).await, want);
    let plan = reconcile(&control_addr).await;
    assert!(plan.replicas.iter().all(|r| r.shard_id != "s0"));
}
