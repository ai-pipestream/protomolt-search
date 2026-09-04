//! Control-plane replica bootstrap (`docs/cluster-control.md`, "Node
//! lifecycle" and "Replica bootstrap"): node A serves shard s0 and keeps
//! ingesting; node B registers empty; the plane plans COPY_REPLICA; B's
//! worker installs from A over StreamSnapshot, catches the WAL tail up
//! with sync_once, reports ready, and completes with counts that match
//! the source — a completion the plane cannot match is refused and
//! retried. The coordinator's live shard map then lists B as s0's
//! replica, a query with A stopped is served by B with the same hits and
//! scores, A's lease expiry promotes B, a restart of B resumes its placed
//! shard at the same address, and a drain of B copies s0 to a third node
//! C and drops B's copy when planned. The copy under A's read lock is
//! measured, and ingest resumes after it.

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::{fit_calibration, unit_vectors, BIT_WIDTH, DIM};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::control_plane::{ClusterControlService, ControlPolicy, DurableControlPlane};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness::serve_node;
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl};
use pipestream_search::node_agent::{rows_of, NodeAgent, NodeAgentConfig, ServedShard};
use pipestream_search::pb::cluster_control_client::ClusterControlClient;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, Bm25SearchRequest, ClusterNodeState, ClusterPlan,
    DrainNodeRequest, FacetValue, FlushRequest, HealthRequest, HealthResponse, PlacementAction,
    PlacementActionKind, ReconcileClusterRequest, SearchRequest, SetCalibrationRequest,
    ShardReplicaRole,
};
use pipestream_search::snapshot::export_snapshot;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DOCS: usize = 40;

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("replica_bootstrap_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The single-image layout keeps a flush independent of an ingest stream
/// in flight (a segment seal wants the tail's documents and vectors
/// aligned, which the two-stream ingest below is not between streams).
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

fn agent_config(
    node_id: &str,
    data_dir: PathBuf,
    control_addr: &str,
    node_addr: &str,
    lease_ms: u64,
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
        lease_ms,
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

const WORDS: [&str; 8] = [
    "court", "appeal", "claim", "denied", "brief", "steps", "reporter", "motion",
];
const COURTS: [&str; 4] = ["scotus", "ca9", "ca5", "nysd"];

fn doc_text(i: usize) -> String {
    (0..(3 + i % 5))
        .map(|j| WORDS[(i * 7 + j * 3) % WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

async fn client(addr: &str) -> NodeServiceClient<tonic::transport::Channel> {
    NodeServiceClient::connect(addr.to_string()).await.unwrap()
}

/// Documents `from..to` with a court facet, then the matching vectors.
async fn ingest(addr: &str, from: usize, to: usize) {
    let mut c = client(addr).await;
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        for i in from..to {
            let _ = tx
                .send(AddDocumentsRequest {
                    text: doc_text(i),
                    analysis: Some(body_spec()),
                    facets: vec![FacetValue {
                        field: "court".into(),
                        value: COURTS[i % COURTS.len()].into(),
                    }],
                    ..Default::default()
                })
                .await;
        }
    });
    let added = c
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
        .added;
    assert_eq!(added as usize, to - from);
    let corpus = unit_vectors(to, DIM, 0x5EED_CA11);
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        let _ = tx
            .send(AddVectorsRequest {
                vectors: corpus[from * DIM..to * DIM].to_vec(),
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
    assert_eq!(added as usize, to - from);
}

async fn flush(addr: &str) {
    client(addr).await.flush(FlushRequest {}).await.unwrap();
}

async fn health(addr: &str) -> HealthResponse {
    client(addr)
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
}

fn counts(h: &HealthResponse) -> (u64, u64, u64, u64) {
    (rows_of(h), h.bm25_docs, h.live_docs, h.deleted_docs)
}

/// The answers the coordinator gives that must not depend on which copy
/// served them.
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
        out.push(format!(
            "{:?}",
            (
                resp.hits
                    .iter()
                    .map(|h| (h.doc_id, h.score.to_bits()))
                    .collect::<Vec<_>>(),
                resp.kth_best.to_bits(),
                resp.facets,
            )
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
        out.push(format!(
            "{:?}",
            resp.hits
                .iter()
                .map(|h| (h.vector_id, h.score.to_bits()))
                .collect::<Vec<_>>()
        ));
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

async fn control_client(addr: &str) -> ClusterControlClient<tonic::transport::Channel> {
    ClusterControlClient::connect(addr.to_string())
        .await
        .unwrap()
}

async fn reconcile(addr: &str) -> ClusterPlan {
    control_client(addr)
        .await
        .reconcile_cluster(ReconcileClusterRequest {
            dry_run: false,
            collection: String::new(),
        })
        .await
        .unwrap()
        .into_inner()
}

fn action_of(
    plan: &ClusterPlan,
    kind: PlacementActionKind,
    target: &str,
) -> Option<PlacementAction> {
    plan.actions
        .iter()
        .find(|a| a.kind == kind as i32 && a.target_node_id == target)
        .cloned()
}

fn record<'a>(
    plan: &'a ClusterPlan,
    node: &str,
) -> Option<&'a pipestream_search::pb::ShardReplicaState> {
    plan.replicas
        .iter()
        .find(|r| r.shard_id == "s0" && r.node_id == node)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_bootstraps_from_the_primary_and_takes_over() {
    let dir = tempdir("bootstrap");
    // Node A: shard s0, seeded, DOCS rows, flushed.
    let node_a = NodeServiceImpl::open(shard_config(dir.join("a/shard")), None, false).unwrap();
    let (addr_a, handle_a) = serve_node(node_a.clone()).await;
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
    ingest(&addr_a, 0, DOCS).await;
    flush(&addr_a).await;

    // The coordinator over s0 at A, with a durable control plane that
    // publishes its topology into the live coordinator.
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
            replication_factor: 2,
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

    // A registers with a short lease and reports s0; B registers empty.
    let agent_a = NodeAgent::new(
        agent_config("a", dir.join("a-data"), &control_addr, &addr_a, 4_000),
        vec![ServedShard::configured(
            "s0",
            node_a.clone(),
            addr_a.clone(),
            Some((0, u64::MAX)),
        )],
    );
    agent_a.register().await.unwrap();
    assert_eq!(agent_a.report_all().await.unwrap(), 1);
    let agent_b = NodeAgent::new(
        agent_config(
            "b",
            dir.join("b-data"),
            &control_addr,
            "http://127.0.0.1:1",
            60_000,
        ),
        Vec::new(),
    );
    agent_b.register().await.unwrap();
    assert!(agent_b.open_placed().await.unwrap().is_empty());
    let plan = reconcile(&control_addr).await;
    let copy = action_of(&plan, PlacementActionKind::CopyReplica, "b").expect("copy planned");
    assert_eq!(
        (copy.shard_id.as_str(), copy.source_node_id.as_str()),
        ("s0", "a")
    );
    assert_eq!(copy.reason, "replication deficit");
    assert_eq!(
        plan.topology.len(),
        1,
        "the plan carries the published routes"
    );

    // A keeps ingesting while B bootstraps.
    let stop = Arc::new(AtomicBool::new(false));
    let ingested = Arc::new(AtomicU64::new(DOCS as u64));
    let ingest_loop = {
        let (stop, ingested, addr) = (Arc::clone(&stop), Arc::clone(&ingested), addr_a.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let i = ingested.load(Ordering::Relaxed) as usize;
                ingest(&addr, i, i + 1).await;
                ingested.store(i as u64 + 1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        })
    };
    // Cost gate: the copy under A's read lock, with ingest running, is
    // bounded, and ingest goes on after it.
    let export = export_snapshot(&addr_a, &dir.join("export")).await.unwrap();
    assert!(
        export.copy_millis < 2_000,
        "copy held the read lock for {} ms",
        export.copy_millis
    );
    let before = ingested.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        ingested.load(Ordering::Relaxed) > before,
        "ingest resumed after the export"
    );
    eprintln!(
        "export under the read lock: {} ms for {} bytes",
        export.copy_millis, export.bytes
    );

    // Right before B completes, A moves on (more rows, flushed) without
    // reporting: B's completion cannot match the plane's record of A.
    {
        let (stop, ingested, addr, ingest_loop) = (
            Arc::clone(&stop),
            Arc::clone(&ingested),
            addr_a.clone(),
            Arc::new(tokio::sync::Mutex::new(Some(ingest_loop))),
        );
        agent_b.set_before_complete(Some(Arc::new(move || {
            let (stop, ingested, addr, ingest_loop) = (
                Arc::clone(&stop),
                Arc::clone(&ingested),
                addr.clone(),
                Arc::clone(&ingest_loop),
            );
            Box::pin(async move {
                stop.store(true, Ordering::Relaxed);
                if let Some(handle) = ingest_loop.lock().await.take() {
                    let _ = handle.await;
                }
                let i = ingested.load(Ordering::Relaxed) as usize;
                ingest(&addr, i, i + 3).await;
                ingested.store(i as u64 + 3, Ordering::Relaxed);
                flush(&addr).await;
            })
        })));
    }
    let started = std::time::Instant::now();
    let error = agent_b.run_once().await.unwrap_err();
    assert!(
        error.contains("refused") && error.contains("differs from its source"),
        "{error}"
    );
    let stats = agent_b.stats();
    assert_eq!(stats.installs.load(Ordering::Relaxed), 1);
    assert_eq!(stats.completion_refusals.load(Ordering::Relaxed), 1);
    assert_eq!(stats.copies_completed.load(Ordering::Relaxed), 0);
    let plan = reconcile(&control_addr).await;
    assert!(
        action_of(&plan, PlacementActionKind::CopyReplica, "b").is_some(),
        "still pending"
    );
    // The copy is caught up to the live source already.
    let addr_b = agent_b.shard_addr("s0").expect("placed");
    assert_eq!(
        counts(&health(&addr_b).await),
        counts(&health(&addr_a).await)
    );
    // Once A reports, the retry completes with matching counts.
    agent_b.set_before_complete(None);
    assert_eq!(agent_a.report_all().await.unwrap(), 1);
    agent_b.run_once().await.unwrap();
    eprintln!("bootstrap of s0 on b: {} ms", started.elapsed().as_millis());
    assert_eq!(stats.copies_completed.load(Ordering::Relaxed), 1);
    assert_eq!(
        stats.installs.load(Ordering::Relaxed),
        1,
        "no second install"
    );
    let plan = reconcile(&control_addr).await;
    assert!(action_of(&plan, PlacementActionKind::CopyReplica, "b").is_none());
    let b_record = record(&plan, "b").expect("b's replica record");
    assert!(b_record.ready);
    assert_eq!(b_record.role, ShardReplicaRole::Replica as i32);
    assert_eq!(b_record.addr, addr_b);
    assert_eq!(b_record.rows, record(&plan, "a").unwrap().rows);
    assert_eq!(b_record.generation, record(&plan, "a").unwrap().generation);
    assert_eq!(
        b_record.analysis_fingerprint,
        record(&plan, "a").unwrap().analysis_fingerprint
    );
    let routes = coordinator.current_topology_routes();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].addr, addr_a);
    assert_eq!(routes[0].replica.as_deref(), Some(addr_b.as_str()));
    assert!(coordinator.current_topology_generation() > 1);
    let placed = agent_b.placed("s0").unwrap();
    assert!(placed.installed && placed.ready && placed.completed_action == copy.action_id);
    assert_eq!(placed.slot_offset, 0);
    assert_eq!((placed.hash_lo, placed.hash_hi), (0, u64::MAX));

    // The same hits and scores from B once A is gone.
    let want = signature(&coordinator).await;
    handle_a.abort();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(NodeServiceClient::connect(addr_a.clone()).await.is_err());
    assert_eq!(signature(&coordinator).await, want, "served by the replica");

    // A restart of B resumes the placed shard at the same address.
    agent_b.stop();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let agent_b = NodeAgent::new(
        agent_config(
            "b",
            dir.join("b-data"),
            &control_addr,
            "http://127.0.0.1:1",
            60_000,
        ),
        Vec::new(),
    );
    assert_eq!(agent_b.open_placed().await.unwrap(), vec!["s0".to_string()]);
    assert_eq!(agent_b.shard_addr("s0").as_deref(), Some(addr_b.as_str()));
    agent_b.register().await.unwrap();
    assert_eq!(agent_b.report_all().await.unwrap(), 1);
    assert_eq!(
        counts(&health(&addr_b).await),
        counts(&health(&addr_b).await)
    );
    assert_eq!(signature(&coordinator).await, want);

    // A's lease expires: the plane promotes B and the query still answers.
    tokio::time::sleep(Duration::from_millis(4_300)).await;
    let plan = reconcile(&control_addr).await;
    assert_eq!(
        plan.nodes.iter().find(|n| n.node_id == "a").unwrap().state,
        ClusterNodeState::Expired as i32
    );
    assert_eq!(
        record(&plan, "b").unwrap().role,
        ShardReplicaRole::Primary as i32
    );
    let routes = coordinator.current_topology_routes();
    assert_eq!(
        (routes[0].addr.as_str(), routes[0].replica.as_deref()),
        (addr_b.as_str(), None)
    );
    assert_eq!(
        signature(&coordinator).await,
        want,
        "served by the promoted replica"
    );
    // B's worker refuses to drop the copy it now serves as primary, and
    // A refuses to drop a configured shard.
    let fake = PlacementAction {
        action_id: 999,
        kind: PlacementActionKind::DropReplica as i32,
        shard_id: "s0".into(),
        source_node_id: "b".into(),
        target_node_id: "b".into(),
        ..Default::default()
    };
    let error = agent_b.execute_drop(&fake, &plan).await.unwrap_err();
    assert!(error.contains("serving primary"), "{error}");
    let error = agent_a
        .execute_drop(
            &PlacementAction {
                target_node_id: "a".into(),
                source_node_id: "a".into(),
                ..fake.clone()
            },
            &plan,
        )
        .await
        .unwrap_err();
    assert!(error.contains("configured statically"), "{error}");
    assert_eq!(agent_b.shard_ids(), vec!["s0".to_string()]);

    // A third node C joins; draining B copies s0 to C, promotes C, and
    // plans the drop of B's copy, which B's worker then removes.
    let agent_c = NodeAgent::new(
        agent_config(
            "c",
            dir.join("c-data"),
            &control_addr,
            "http://127.0.0.1:2",
            60_000,
        ),
        Vec::new(),
    );
    agent_c.register().await.unwrap();
    let lease_b = agent_b.lease().unwrap();
    let plan = control_client(&control_addr)
        .await
        .drain_node(DrainNodeRequest {
            node_id: "b".into(),
            lease_token: lease_b.lease_token,
            collection: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let copy = action_of(&plan, PlacementActionKind::CopyReplica, "c").expect("copy to c planned");
    assert_eq!(
        (copy.source_node_id.as_str(), copy.reason.as_str()),
        ("b", "graceful drain")
    );
    agent_c.run_once().await.unwrap();
    assert_eq!(agent_c.stats().copies_completed.load(Ordering::Relaxed), 1);
    let addr_c = agent_c.shard_addr("s0").unwrap();
    assert_eq!(
        counts(&health(&addr_c).await),
        counts(&health(&addr_b).await)
    );
    let plan = reconcile(&control_addr).await;
    assert_eq!(
        record(&plan, "c").unwrap().role,
        ShardReplicaRole::Primary as i32
    );
    let drop = action_of(&plan, PlacementActionKind::DropReplica, "b").expect("drop of b planned");
    assert_eq!(drop.shard_id, "s0");
    agent_b.run_once().await.unwrap();
    assert_eq!(agent_b.stats().drops_completed.load(Ordering::Relaxed), 1);
    assert!(agent_b.shard_ids().is_empty());
    assert!(
        !dir.join("b-data/s0").exists(),
        "the dropped copy's files are gone"
    );
    assert!(NodeServiceClient::connect(addr_b.clone()).await.is_err());
    let plan = reconcile(&control_addr).await;
    assert!(record(&plan, "b").is_none());
    assert!(action_of(&plan, PlacementActionKind::DropReplica, "b").is_none());
    let routes = coordinator.current_topology_routes();
    assert_eq!(
        (routes[0].addr.as_str(), routes[0].replica.as_deref()),
        (addr_c.as_str(), None)
    );
    assert_eq!(signature(&coordinator).await, want, "served by c");
    agent_c.stop();
    let _ = std::fs::remove_dir_all(&dir);
}
