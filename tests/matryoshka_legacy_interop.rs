//! Opt-in interoperability proof against the pre-retention b20c74f1 reader.
//!
//! Set PSEARCH_LEGACY_BINARY to the verified existing executable and run this
//! ignored test explicitly. No historical checkout or build is performed.
#![cfg(feature = "net")]

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::link::NodeLink;
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, FlushRequest, HealthRequest, HealthResponse,
};
use pipestream_search::sha256::Sha256;

const LEGACY_REVISION: &str = "b20c74f1bf393b99499d8609c4828f125a4db6a6";
const LEGACY_SHA256: &str = "50dc741d4318102dcf9ba9d2bc31148db2c9c735bd13a682a62e3addf50fb151";
const STARTUP_BOUND: Duration = Duration::from_secs(10);
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let parent = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
        std::fs::create_dir_all(&parent).unwrap();
        let path = parent.join(format!(
            "matryoshka-legacy-interop-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        // Ownership is established only by successful exclusive creation.
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn verified_legacy_binary() -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os("PSEARCH_LEGACY_BINARY")
            .expect("set PSEARCH_LEGACY_BINARY to the verified pre-retention executable"),
    )
    .canonicalize()
    .unwrap();
    let mut file = File::open(&path).unwrap();
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    let actual = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        actual,
        LEGACY_SHA256,
        "unexpected legacy executable: {}",
        path.display()
    );
    path
}

/// Produce both fixtures with the current writer and close every current-node
/// handle before the old process opens the persisted generation.
async fn fixture(directory: &Path, retained: bool) -> PathBuf {
    std::fs::create_dir(directory).unwrap();
    let path = directory.join("index");
    let config = NodeConfig {
        index_path: Some(path.clone()),
        layout: Layout::SingleImage,
        wal: false,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        matryoshka_prefix_dims: if retained { vec![8, 16] } else { Vec::new() },
        matryoshka_active_dim: retained.then_some(8),
        ..Default::default()
    };
    let index = pipestream_search::harness::seeded_index(8, 4, &[0.0; 8], &[1.0; 8]);
    let node = Arc::new(NodeServiceImpl::new(Some(index), config.clone()));
    let mut link = NodeLink::local(Arc::clone(&node));
    // The two-call ingest contract assigns document slots before the matching
    // vector append; vectors first would place the documents after that tip.
    link.add_documents(tokio_stream::iter((0..3).map(|row| AddDocumentsRequest {
        text: format!("legacy interop document {row}"),
        analysis: Some(body_spec()),
        ..Default::default()
    })))
    .await
    .unwrap();
    let dim = if retained { 16 } else { 8 };
    let vectors = (0..3)
        .flat_map(|row| {
            let mut vector = vec![0.0; dim];
            vector[0] = 0.6 + row as f32 * 0.03;
            if retained {
                vector[8] = 0.8 - row as f32 * 0.02;
            }
            vector
        })
        .collect();
    link.add_vectors(tokio_stream::iter([AddVectorsRequest {
        vectors,
        dim: dim as u32,
        ..Default::default()
    }]))
    .await
    .unwrap();
    link.flush(FlushRequest {}).await.unwrap();
    assert_eq!(node.multi_res_row(0).is_some(), retained);
    drop(link);
    drop(node);
    assert!(!pipestream_search::wal::wal_dir(&path).exists());
    let live = std::fs::read(pipestream_search::node::live_docs_sidecar_path(&path)).unwrap();
    assert_eq!(
        u32::from_le_bytes(live[8..12].try_into().unwrap()),
        if retained { 2 } else { 1 }
    );
    assert_eq!(directory.join("index.multi").is_file(), retained);
    assert_eq!(directory.join("index.multi.json").is_file(), retained);
    let reopened = NodeServiceImpl::open(config, None, false)
        .expect("the current reader must accept its own complete fixture");
    assert_eq!(reopened.multi_res_row(0).is_some(), retained);
    drop(reopened);
    path
}

struct LegacyNode {
    child: Child,
    stdout: PathBuf,
    stderr: PathBuf,
    addr: SocketAddr,
}

impl LegacyNode {
    fn spawn(binary: &Path, index: &Path) -> Self {
        let directory = index.parent().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let stdout = directory.join("legacy-stdout.log");
        let stderr = directory.join("legacy-stderr.log");
        let log = |path: &Path| {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .unwrap()
        };
        let child = Command::new(binary)
            .current_dir(directory)
            .arg("--role=node")
            .arg(format!("--index={}", index.display()))
            .arg(format!("--node-listen={addr}"))
            .arg("--wal=false")
            .arg("--analysis-addr=native://")
            .arg("--layout=single-image")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log(&stdout)))
            .stderr(Stdio::from(log(&stderr)))
            .spawn()
            .unwrap();
        Self {
            child,
            stdout,
            stderr,
            addr,
        }
    }

    fn diagnostics(&self) -> String {
        let read = |path: &Path| {
            let mut text = String::new();
            if let Ok(file) = File::open(path) {
                let _ = file.take(64 * 1024).read_to_string(&mut text);
            }
            text
        };
        format!(
            "stdout:\n{}\nstderr:\n{}",
            read(&self.stdout),
            read(&self.stderr)
        )
    }

    async fn exit(&mut self) -> ExitStatus {
        tokio::time::timeout(STARTUP_BOUND, async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("legacy node failed to exit: {}", self.diagnostics()))
    }

    async fn health(&mut self) -> HealthResponse {
        let endpoint = format!("http://{}", self.addr);
        tokio::time::timeout(STARTUP_BOUND, async {
            loop {
                assert!(
                    self.child.try_wait().unwrap().is_none(),
                    "legacy node exited before Health: {}",
                    self.diagnostics()
                );
                let probe = tokio::time::timeout(Duration::from_millis(250), async {
                    let mut client = NodeServiceClient::connect(endpoint.clone())
                        .await
                        .map_err(|error| error.to_string())?;
                    client
                        .health(HealthRequest {})
                        .await
                        .map(|reply| reply.into_inner())
                        .map_err(|error| error.to_string())
                })
                .await;
                if let Ok(Ok(health)) = probe {
                    assert!(
                        self.child.try_wait().unwrap().is_none(),
                        "legacy child exited during Health: {}",
                        self.diagnostics()
                    );
                    return health;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("legacy node never became healthy: {}", self.diagnostics()))
    }
}

impl Drop for LegacyNode {
    fn drop(&mut self) {
        // Only the process created by this guard is ever signalled. Always
        // reap it before the enclosing Directory removes its fixture/logs.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PSEARCH_LEGACY_BINARY pointing to the verified b20c74f1 executable"]
async fn old_reader_refuses_retained_generation_and_serves_plain_v1_control() {
    let binary = verified_legacy_binary();
    let root = Directory::new();
    let retained = fixture(&root.0.join("retained"), true).await;
    let plain = fixture(&root.0.join("plain"), false).await;

    let mut refusing = LegacyNode::spawn(&binary, &retained);
    let status = refusing.exit().await;
    assert!(
        !status.success() && status.code().is_some(),
        "expected an ordinary nonzero startup refusal: {status}; {}",
        refusing.diagnostics()
    );
    assert!(
        refusing
            .diagnostics()
            .contains("unsupported live-doc version"),
        "startup failed for another reason: {}",
        refusing.diagnostics()
    );
    drop(refusing);

    let mut serving = LegacyNode::spawn(&binary, &plain);
    let health = serving.health().await;
    assert_eq!(health.num_vectors, 3, "{}", serving.diagnostics());
    assert_eq!(health.bm25_docs, 3, "{}", serving.diagnostics());
    assert_eq!(health.multi_res_vector_version, 0);
    drop(serving);
    eprintln!("legacy interop: revision={LEGACY_REVISION} sha256={LEGACY_SHA256} retained_live_version=2 exit={status} plain_live_version=1 vectors={} documents={}", health.num_vectors, health.bm25_docs);
}
