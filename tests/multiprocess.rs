//! Multi-process acceptance test (phase 3): the write path and calibration
//! distribution across real OS processes on one machine.
//!
//! Scenario: two node processes + one coordinator process (the actual
//! binary, temp dirs and ports). The test pushes the SAME seeded
//! calibration to both nodes via SetCalibration, ingests disjoint vector
//! sets over AddVectors, and requires the coordinator's top-k to equal the
//! monolithic reference EXACTLY — losslessness for data that arrived over
//! the wire, not just prebuilt indexes. Then one node is SIGTERMed (proving
//! save-on-shutdown persistence) and restarted, and the search is
//! re-verified against the reloaded shard.

mod common;

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{AddVectorsRequest, FlushRequest, SearchRequest, SetCalibrationRequest};

use common::{monolithic_topk, unit_vectors, BIT_WIDTH, DIM};

const BIN: &str = env!("CARGO_BIN_EXE_turbovec-search");
const SHARD_VECTORS: usize = 3_000;

/// Kill-on-drop guard so a failed assertion never leaks server processes.
struct Proc(Child);

impl Proc {
    fn spawn(args: &[String]) -> Self {
        Proc(
            Command::new(BIN)
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn turbovec-search"),
        )
    }

    fn pid(&self) -> u32 {
        self.0.id()
    }

    /// SIGTERM, then wait for exit (save-on-shutdown must run first).
    fn terminate(mut self) {
        let _ = Command::new("/bin/kill")
            .args(["-TERM", &self.pid().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match self.0.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50))
                }
                Ok(None) => panic!("process {} did not exit after SIGTERM", self.pid()),
                Err(e) => panic!("wait on {}: {e}", self.pid()),
            }
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn node_args(port: u16, index: &Path, slot_offset: u64) -> Vec<String> {
    vec![
        "--role=node".into(),
        format!("--node-listen=127.0.0.1:{port}"),
        format!("--index={}", index.display()),
        format!("--slot-offset={slot_offset}"),
        "--chunk-blocks=8".into(),
    ]
}

async fn wait_ready(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if NodeServiceClient::connect(format!("http://{addr}"))
            .await
            .is_ok()
        {
            return;
        }
        assert!(Instant::now() < deadline, "node at {addr} never came up");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn add_vectors(addr: &str, vectors: Vec<f32>) -> turbovec_search::pb::AddVectorsResponse {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    // Two batches, to exercise the streaming path.
    let half = vectors.len() / 2;
    tx.send(AddVectorsRequest {
        vectors: vectors[..half].to_vec(),
        dim: 0,
    })
    .await
    .unwrap();
    tx.send(AddVectorsRequest {
        vectors: vectors[half..].to_vec(),
        dim: 0,
    })
    .await
    .unwrap();
    drop(tx);
    client
        .add_vectors(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
}

async fn coordinator_topk(coord: &str, query: &[f32], k: u32) -> Vec<(u64, u32)> {
    let mut client = SearchServiceClient::connect(coord.to_string())
        .await
        .unwrap();
    let response = client
        .search(SearchRequest {
            request_id: String::new(),
            k,
            vector: query.to_vec(),
            collapse_parents: false,
        })
        .await
        .unwrap()
        .into_inner();
    response
        .hits
        .iter()
        .map(|h| (h.vector_id, h.score.to_bits()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingest_across_processes_is_lossless_and_persistent() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("turbovec_mp_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let shard0_path = dir.join("shard-0.tv");
    let shard1_path = dir.join("shard-1.tv");

    let (p0, p1, pc) = (free_port(), free_port(), free_port());
    let node0 = format!("127.0.0.1:{p0}");
    let node1 = format!("127.0.0.1:{p1}");
    let coord = format!("127.0.0.1:{pc}");

    let proc0 = Proc::spawn(&node_args(p0, &shard0_path, 0));
    let mut proc1 = Proc::spawn(&node_args(p1, &shard1_path, SHARD_VECTORS as u64));
    let _procc = Proc::spawn(&[
        "--role=coordinator".into(),
        format!("--coord-listen={coord}"),
        format!("--nodes={node0},{node1}"),
    ]);
    wait_ready(&node0).await;
    wait_ready(&node1).await;

    // Fit one calibration and push it to both nodes.
    let corpus = unit_vectors(2 * SHARD_VECTORS, DIM, 0x9C33_0001);
    let (shift, scale) = common::fit_calibration(DIM, BIT_WIDTH, &corpus[..2_000 * DIM]);
    for addr in [&node0, &node1] {
        let mut client = NodeServiceClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let resp = client
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: BIT_WIDTH as u32,
                shift: shift.clone(),
                scale: scale.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.already_seeded, "first seed must lock, not no-op");
    }

    // Ingest disjoint halves over the wire.
    let resp0 = add_vectors(
        &format!("http://{node0}"),
        corpus[..SHARD_VECTORS * DIM].to_vec(),
    )
    .await;
    let resp1 = add_vectors(
        &format!("http://{node1}"),
        corpus[SHARD_VECTORS * DIM..].to_vec(),
    )
    .await;
    assert_eq!((resp0.added, resp0.total, resp0.first_id), (3000, 3000, 0));
    assert_eq!(
        (resp1.added, resp1.total, resp1.first_id),
        (3000, 3000, 3000)
    );

    // Monolithic reference: same corpus, same calibration.
    let monolithic =
        turbovec_search::harness::build_monolithic(&corpus, DIM, BIT_WIDTH, &shift, &scale);

    // Lossless over ingested data, for several queries and two k values.
    let coord_http = format!("http://{coord}");
    for qi in 0..3u64 {
        for k in [10u32, 100] {
            let query = unit_vectors(1, DIM, 0x9C33_1000 + qi);
            let got = coordinator_topk(&coord_http, &query, k).await;
            let want = monolithic_topk(&monolithic, &query, k as usize);
            assert_eq!(
                got, want,
                "query {qi} k={k}: ingested cluster != monolithic"
            );
        }
    }

    // Flush works on demand (node 0) ...
    let flushed = NodeServiceClient::connect(format!("http://{node0}"))
        .await
        .unwrap()
        .flush(FlushRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(flushed.written && flushed.num_vectors == 3_000);
    assert!(shard0_path.exists());

    // ... and save-on-shutdown persists node 1: SIGTERM, restart, re-verify.
    assert!(!shard1_path.exists(), "no flush expected before shutdown");
    let pid = proc1.pid();
    proc1.terminate();
    assert!(shard1_path.exists(), "save-on-shutdown wrote no index");

    proc1 = Proc::spawn(&node_args(p1, &shard1_path, SHARD_VECTORS as u64));
    wait_ready(&node1).await;
    let cal = NodeServiceClient::connect(format!("http://{node1}"))
        .await
        .unwrap()
        .get_calibration(turbovec_search::pb::GetCalibrationRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        cal.num_vectors, 3_000,
        "restarted node lost vectors (pid was {pid})"
    );
    assert_eq!(cal.shift, shift, "restarted node lost its calibration");

    let query = unit_vectors(1, DIM, 0x9C33_1000);
    let got = coordinator_topk(&coord_http, &query, 10).await;
    let want = monolithic_topk(&monolithic, &query, 10);
    assert_eq!(got, want, "after restart: cluster != monolithic");

    drop(proc0);
    drop(proc1);
    let _ = std::fs::remove_dir_all(&dir);
}

/// An interrupted BM25 build must stop the node, not be served quietly.
///
/// `Flush` removes the spill directory on success, so a `.bm25.build`
/// with no `.bm25` beside it cannot be reached by a shard that finished.
/// Coming up anyway is the failure that hides: the node is healthy, the
/// vector leg is correct, and every lexical query silently ranks against
/// a corpus missing this shard's share -- which reads exactly like a
/// corpus that never held those terms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_interrupted_bm25_build_refuses_to_serve() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("tv_interrupted_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let index = dir.join("shard.tv");

    // A real index with vectors, then the wreckage of a build.
    let port = free_port();
    {
        let node = Proc::spawn(&node_args(port, &index, 0));
        let addr = format!("127.0.0.1:{port}");
        wait_ready(&addr).await;
        let corpus = unit_vectors(256, DIM, 0x9C33_0009);
        let (shift, scale) = common::fit_calibration(DIM, BIT_WIDTH, &corpus);
        NodeServiceClient::connect(format!("http://{addr}"))
            .await
            .unwrap()
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: BIT_WIDTH as u32,
                shift,
                scale,
            })
            .await
            .unwrap();
        add_vectors(&format!("http://{addr}"), corpus).await;
        // SIGTERM so save-on-shutdown runs; a kill would leave no index.
        node.terminate();
    }
    assert!(index.exists(), "the shard should have saved its vectors");
    let build = turbovec_search::node::bm25_build_dir(
        &turbovec_search::node::bm25_sidecar_path(&index),
    );
    std::fs::create_dir_all(&build).unwrap();

    // Refused: the process exits rather than serving a half-built shard.
    let port2 = free_port();
    let out = Command::new(BIN)
        .args(node_args(port2, &index, 0))
        .output()
        .expect("spawn node");
    assert!(!out.status.success(), "a half-built shard must not serve");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("interrupted") && stderr.contains("allow-missing-bm25"),
        "the refusal must say what happened and how to override: {stderr}"
    );

    // And the override really does override.
    let port3 = free_port();
    let mut args = node_args(port3, &index, 0);
    args.push("--allow-missing-bm25".into());
    let node = Proc::spawn(&args);
    wait_ready(&format!("127.0.0.1:{port3}")).await;
    drop(node);
    std::fs::remove_dir_all(&dir).ok();
}
