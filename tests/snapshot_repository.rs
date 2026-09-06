//! The snapshot repository (`docs/snapshots.md`): `ExportSnapshot` writes
//! a directory with a hashed manifest; `InstallSnapshotFrom` pulls it from
//! that directory, from a peer's `StreamSnapshot`, or over HTTP with a
//! `Range` resume, verifies every artifact, and installs through the same
//! path the client stream takes. Every install is held bitwise to the
//! source (vector Search, BM25 with facets and filters, health counts),
//! tampering and the wrong manifest digest refuse by name, both layouts
//! export and install and never mix, a foreign scoring fingerprint
//! refuses, and the manifest's WAL cutoff is exactly where `sync_once`
//! resumes.

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use common::{fit_calibration, start_empty_node, unit_vectors, BIT_WIDTH, DIM};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::install_snapshot_from_request::Source;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, Bm25SearchRequest, ExportSnapshotResponse, FacetValue,
    FlushRequest, HealthRequest, HealthResponse, InstallSnapshotFromRequest,
    InstallSnapshotResponse, SearchRequest, SetCalibrationRequest,
};
use pipestream_search::replication::{sync_once, ReplicaCursor};
use pipestream_search::snapshot::{export_snapshot, install_snapshot_from};
use pipestream_search::snapshot_repository::{
    self as repo, RepositoryManifest, LAYOUT_SEGMENTS, LAYOUT_SINGLE_IMAGE, MANIFEST_FILE,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request};

const DOCS: usize = 24;
const SLOT: u64 = 1_000;

fn tempdir(tag: &str) -> PathBuf {
    // CARGO_TARGET_TMPDIR lives under target/ (a real disk), not the
    // tmpfs /tmp — index files in tests must not consume RAM.
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("snapshot_repo_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: PathBuf, layout: Layout) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        facet_fields: vec!["court".to_string()],
        layout,
        wal: true,
        slot_offset: SLOT,
        ..Default::default()
    }
}

const WORDS: [&str; 8] = [
    "court", "appeal", "claim", "denied", "brief", "steps", "reporter", "motion",
];
const COURTS: [&str; 4] = ["scotus", "ca9", "ca5", "nysd"];

fn doc_text(i: usize) -> String {
    let mut words = Vec::new();
    for j in 0..(3 + i % 5) {
        words.push(WORDS[(i * 7 + j * 3) % WORDS.len()]);
    }
    words.join(" ")
}

async fn client(addr: &str) -> NodeServiceClient<tonic::transport::Channel> {
    NodeServiceClient::connect(addr.to_string()).await.unwrap()
}

async fn seed(addr: &str) {
    let corpus = unit_vectors(2_000, DIM, 0xCA11_0001);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus);
    client(addr)
        .await
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
}

/// Ingest documents `from..to` (text plus a court facet) and the
/// matching vectors, one row each, so both layouts stay aligned.
async fn ingest(addr: &str, from: usize, to: usize) {
    let mut c = client(addr).await;
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        for i in from..to {
            tx.send(AddDocumentsRequest {
                text: doc_text(i),
                analysis: Some(body_spec()),
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: COURTS[i % COURTS.len()].into(),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
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
        tx.send(AddVectorsRequest {
            vectors: corpus[from * DIM..to * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
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
    assert!(
        client(addr)
            .await
            .flush(FlushRequest {})
            .await
            .unwrap()
            .into_inner()
            .written
    );
}

async fn health(addr: &str) -> HealthResponse {
    client(addr)
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
}

/// The numbers a snapshot must carry over.
fn counts(h: &HealthResponse) -> (u64, u64, u64, u64, u64, String) {
    (
        h.num_vectors,
        h.document_slots,
        h.bm25_docs,
        h.live_docs,
        h.deleted_docs,
        h.scoring_fingerprint.clone(),
    )
}

fn coordinator(addr: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![addr.to_string()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
}

/// Every answer the shard gives that must not depend on where its bytes
/// came from: BM25 with facets and filters, and vector Search.
async fn signature(addr: &str) -> Vec<String> {
    let c = coordinator(addr);
    let base = |text: &str| Bm25SearchRequest {
        text: text.to_string(),
        k: 30,
        analysis: Some(body_spec()),
        ..Default::default()
    };
    let probes = vec![
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
    ];
    let mut out = Vec::new();
    for probe in probes {
        let resp = SearchService::bm25_search(&c, Request::new(probe))
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
    let corpus = unit_vectors(4, DIM, 0x0E0E_0001);
    for q in 0..4 {
        let resp = SearchService::search(
            &c,
            Request::new(SearchRequest {
                k: 10,
                vector: corpus[q * DIM..(q + 1) * DIM].to_vec(),
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

/// Serve an already-opened shard on a fresh loopback listener.
async fn serve(node: NodeServiceImpl) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    node.spawn_floor_listener(listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(node.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener))
            .await;
    });
    (addr, handle)
}

fn manifest_of(export: &ExportSnapshotResponse) -> RepositoryManifest {
    RepositoryManifest::from_pb(export.manifest.as_ref().unwrap()).unwrap()
}

async fn install(
    addr: &str,
    source: Source,
    expected_sha: &str,
) -> Result<InstallSnapshotResponse, tonic::Status> {
    install_snapshot_from(
        addr,
        InstallSnapshotFromRequest {
            source: Some(source),
            expected_manifest_sha256: expected_sha.to_string(),
            bearer_token: String::new(),
        },
    )
    .await
}

/// A source shard: single-image layout, seeded, DOCS rows, flushed.
async fn source_shard(
    dir: &Path,
    layout: Layout,
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    PathBuf,
) {
    let path = dir.join("source.tv");
    let (addr, handle) = start_empty_node(config(path.clone(), layout)).await;
    seed(&addr).await;
    if layout == Layout::Segments {
        ingest(&addr, 0, DOCS / 2).await;
        flush(&addr).await;
        ingest(&addr, DOCS / 2, DOCS).await;
    } else {
        ingest(&addr, 0, DOCS).await;
    }
    flush(&addr).await;
    (addr, handle, path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_writes_a_hashed_manifest_and_refuses_a_non_empty_directory() {
    let dir = tempdir("export");
    let (source, handle, _) = source_shard(&dir, Layout::SingleImage).await;
    let repo_dir = dir.join("repo");
    let export = export_snapshot(&source, &repo_dir).await.unwrap();
    let manifest = manifest_of(&export);
    assert_eq!(manifest.layout, LAYOUT_SINGLE_IMAGE);
    assert_eq!(manifest.format_version, repo::FORMAT_VERSION);
    let h = health(&source).await;
    assert_eq!(manifest.slot_offset, SLOT);
    assert_eq!(manifest.vector_rows, h.num_vectors);
    assert_eq!(manifest.document_rows, h.document_slots);
    assert_eq!(manifest.live_rows, h.live_docs);
    assert_eq!(manifest.scoring_fingerprint, h.scoring_fingerprint);
    assert_eq!(manifest.backend_kind, h.vector_backend);
    assert_eq!(manifest.dim as usize, DIM);
    assert!(manifest.wal_clocked);
    assert_eq!(
        (manifest.wal_generation, manifest.wal_high_watermark),
        (h.wal_generation, h.wal_high_watermark)
    );
    assert_eq!(manifest.analysis_fingerprints.len(), 1);
    assert_ne!(manifest.analysis_fingerprints[0], 0);
    // Every artifact is on disk with the declared size and digest, and
    // the manifest file's own digest is what the response reports.
    let names: Vec<&str> = manifest.artifacts.iter().map(|a| a.file.as_str()).collect();
    assert!(
        names.contains(&"vector.index") && names.contains(&"documents.bm25"),
        "{names:?}"
    );
    let mut total = 0;
    for artifact in &manifest.artifacts {
        let (bytes, sha) = repo::hash_file(&repo_dir.join(&artifact.file)).unwrap();
        assert_eq!(
            (bytes, sha),
            (artifact.bytes, artifact.sha256.clone()),
            "{}",
            artifact.file
        );
        total += bytes;
    }
    assert_eq!(export.bytes, total);
    let (parsed, sha) = repo::read_manifest(&repo_dir).unwrap();
    assert_eq!(parsed, manifest);
    assert_eq!(sha, export.manifest_sha256);
    assert_eq!(
        export.manifest_path,
        repo_dir.join(MANIFEST_FILE).display().to_string()
    );
    // The repository is exactly the artifacts plus the manifest.
    let files = repo::walk_files(&repo_dir).unwrap();
    assert_eq!(files.len(), manifest.artifacts.len() + 1);
    // A non-empty directory refuses by name; so does one with a stray file.
    let error = export_snapshot(&source, &repo_dir).await.unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("is not empty"),
        "{}",
        error.message()
    );
    let stray = dir.join("stray");
    std::fs::create_dir_all(&stray).unwrap();
    std::fs::write(stray.join("note.txt"), b"x").unwrap();
    let error = export_snapshot(&source, &stray).await.unwrap_err();
    assert!(
        error.message().contains("(1 entries)"),
        "{}",
        error.message()
    );
    // A shard without a persistence path has nothing to export.
    let (memory, memory_handle) = start_empty_node(NodeConfig::default()).await;
    let error = export_snapshot(&memory, &dir.join("memory"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("no persistence path"),
        "{}",
        error.message()
    );
    memory_handle.abort();
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn install_from_directory_equals_the_source_and_refuses_tampering() {
    let dir = tempdir("directory");
    let (source, source_handle, _) = source_shard(&dir, Layout::SingleImage).await;
    let want = signature(&source).await;
    let want_counts = counts(&health(&source).await);
    let repo_dir = dir.join("repo");
    let export = export_snapshot(&source, &repo_dir).await.unwrap();
    let manifest = manifest_of(&export);

    // A fresh, unseeded shard at the same slot offset adopts the image.
    let target_path = dir.join("target.tv");
    let (target, target_handle) =
        start_empty_node(config(target_path.clone(), Layout::SingleImage)).await;
    let installed = install(
        &target,
        Source::Directory(repo_dir.display().to_string()),
        &export.manifest_sha256,
    )
    .await
    .unwrap();
    assert_eq!(installed.num_vectors, DOCS as u64);
    assert_eq!(installed.num_documents, DOCS as u64);
    assert_eq!(
        RepositoryManifest::from_pb(installed.manifest.as_ref().unwrap()).unwrap(),
        manifest
    );
    assert_eq!(counts(&health(&target).await), want_counts);
    assert_eq!(signature(&target).await, want);

    // Persistence: reopen the installed shard from disk, same answers.
    target_handle.abort();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let reopened = NodeServiceImpl::open(
        config(target_path.clone(), Layout::SingleImage),
        None,
        false,
    )
    .unwrap();
    let (again, again_handle) = serve(reopened).await;
    assert_eq!(counts(&health(&again).await), want_counts);
    assert_eq!(signature(&again).await, want);
    again_handle.abort();

    // Refusals, each by name, each leaving the target untouched.
    let fresh_path = dir.join("fresh.tv");
    let (fresh, fresh_handle) =
        start_empty_node(config(fresh_path.clone(), Layout::SingleImage)).await;
    let tampered = dir.join("tampered");
    for artifact in &manifest.artifacts {
        let to = tampered.join(&artifact.file);
        std::fs::create_dir_all(to.parent().unwrap()).unwrap();
        std::fs::copy(repo_dir.join(&artifact.file), &to).unwrap();
    }
    std::fs::copy(repo_dir.join(MANIFEST_FILE), tampered.join(MANIFEST_FILE)).unwrap();
    {
        // Flip one byte in the middle of the BM25 image: same size.
        let path = tampered.join("documents.bm25");
        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
    }
    let error = install(
        &fresh,
        Source::Directory(tampered.display().to_string()),
        "",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("\"documents.bm25\" hashes to"),
        "{}",
        error.message()
    );
    // A truncated artifact names its size.
    {
        let path = tampered.join("documents.bm25");
        std::fs::copy(repo_dir.join("documents.bm25"), &path).unwrap();
        let path = tampered.join("vector.index");
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();
    }
    let error = install(
        &fresh,
        Source::Directory(tampered.display().to_string()),
        "",
    )
    .await
    .unwrap_err();
    assert!(
        error.message().contains("\"vector.index\" has") && error.message().contains("bytes"),
        "{}",
        error.message()
    );
    // The wrong manifest digest refuses before anything is verified.
    let error = install(
        &fresh,
        Source::Directory(repo_dir.display().to_string()),
        &"0".repeat(64),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("manifest sha256 is"),
        "{}",
        error.message()
    );
    // A missing repository, an empty request, and another shard's offset.
    let error = install(
        &fresh,
        Source::Directory(dir.join("nowhere").display().to_string()),
        "",
    )
    .await
    .unwrap_err();
    assert!(
        error.message().contains(MANIFEST_FILE),
        "{}",
        error.message()
    );
    let error = install_snapshot_from(&fresh, InstallSnapshotFromRequest::default())
        .await
        .unwrap_err();
    assert!(
        error.message().contains("needs a source"),
        "{}",
        error.message()
    );
    let (other, other_handle) = start_empty_node(NodeConfig {
        slot_offset: SLOT + 1,
        ..config(dir.join("other.tv"), Layout::SingleImage)
    })
    .await;
    let error = install(
        &other,
        Source::Directory(repo_dir.display().to_string()),
        "",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains(&format!(
            "slot offset {SLOT}, this shard serves {}",
            SLOT + 1
        )),
        "{}",
        error.message()
    );
    // Nothing was installed on the fresh shard, and no staging is left.
    let h = health(&fresh).await;
    assert_eq!((h.num_vectors, h.document_slots), (0, 0));
    assert!(!fresh_path.with_extension("tv.snap").exists());
    assert!(!PathBuf::from(format!("{}.snap-tmp", fresh_path.display())).exists());
    other_handle.abort();
    fresh_handle.abort();
    source_handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn install_from_a_peer_equals_the_source() {
    let dir = tempdir("peer");
    let (source, source_handle, source_path) = source_shard(&dir, Layout::SingleImage).await;
    let want = signature(&source).await;
    let want_counts = counts(&health(&source).await);
    let (target, target_handle) =
        start_empty_node(config(dir.join("target.tv"), Layout::SingleImage)).await;
    let installed = install(&target, Source::PeerAddr(source.clone()), "")
        .await
        .unwrap();
    let manifest = RepositoryManifest::from_pb(installed.manifest.as_ref().unwrap()).unwrap();
    assert_eq!(manifest.layout, LAYOUT_SINGLE_IMAGE);
    assert_eq!(manifest.vector_rows, DOCS as u64);
    assert_eq!(counts(&health(&target).await), want_counts);
    assert_eq!(signature(&target).await, want);
    // The peer's export staging is gone, and the peer still serves.
    let parent = source_path.parent().unwrap();
    let staging: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains("snap-export"))
        .collect();
    assert!(staging.is_empty(), "{staging:?}");
    assert_eq!(signature(&source).await, want);
    // The digest the peer form pins is the canonical manifest's; a second
    // export flushes again (one more WAL marker), so only the shape is
    // pinned here, the directory test pins the value.
    let error = install(&target, Source::PeerAddr(source.clone()), &"1".repeat(64))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("manifest sha256 is"),
        "{}",
        error.message()
    );
    // An unreachable peer refuses by name.
    let error = install(&target, Source::PeerAddr("http://127.0.0.1:1".into()), "")
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    target_handle.abort();
    source_handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A minimal HTTP/1.1 file server over `root`: GET with `Range:
/// bytes=N-` (206 + Content-Range), a bearer check when `bearer` is set,
/// one request per connection. With `drop_first`, the first full GET of
/// an artifact declares its full length and closes after 64 KiB, so the
/// client has to resume with a Range request.
struct FileServer {
    root: PathBuf,
    bearer: Option<String>,
    drop_first: AtomicBool,
    range_requests: AtomicU64,
    requests: AtomicU64,
}

async fn serve_files(server: Arc<FileServer>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                        return;
                    }
                    head.push(byte[0]);
                }
                let head = String::from_utf8_lossy(&head).into_owned();
                let mut lines = head.split("\r\n");
                let request_line = lines.next().unwrap_or("");
                let mut parts = request_line.split(' ');
                let method = parts.next().unwrap_or("");
                let path = parts.next().unwrap_or("/");
                let mut range = None;
                let mut authorization = None;
                for line in lines {
                    if let Some((name, value)) = line.split_once(':') {
                        match name.trim().to_ascii_lowercase().as_str() {
                            "range" => range = Some(value.trim().to_string()),
                            "authorization" => authorization = Some(value.trim().to_string()),
                            _ => {}
                        }
                    }
                }
                server.requests.fetch_add(1, Ordering::Relaxed);
                let reply = |status: &str, body: &[u8]| {
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes()
                    .into_iter()
                    .chain(body.iter().copied())
                    .collect::<Vec<u8>>()
                };
                if method != "GET" {
                    let _ = stream
                        .write_all(&reply("405 Method Not Allowed", b""))
                        .await;
                    return;
                }
                if let Some(expected) = &server.bearer {
                    if authorization.as_deref() != Some(&format!("Bearer {expected}")) {
                        let _ = stream.write_all(&reply("401 Unauthorized", b"no")).await;
                        return;
                    }
                }
                let file = server.root.join(path.trim_start_matches('/'));
                let Ok(bytes) = std::fs::read(&file) else {
                    let _ = stream.write_all(&reply("404 Not Found", b"")).await;
                    return;
                };
                let is_artifact = !path.ends_with(MANIFEST_FILE);
                match range {
                    Some(range) => {
                        server.range_requests.fetch_add(1, Ordering::Relaxed);
                        let from: usize = range
                            .strip_prefix("bytes=")
                            .and_then(|r| r.strip_suffix('-'))
                            .and_then(|n| n.parse().ok())
                            .unwrap_or(0);
                        let tail = &bytes[from.min(bytes.len())..];
                        let head = format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {from}-{}/{}\r\nConnection: close\r\n\r\n",
                            tail.len(),
                            bytes.len().saturating_sub(1),
                            bytes.len()
                        );
                        let _ = stream.write_all(head.as_bytes()).await;
                        let _ = stream.write_all(tail).await;
                    }
                    None if is_artifact
                        && bytes.len() > 64 * 1024
                        && server.drop_first.swap(false, Ordering::Relaxed) =>
                    {
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            bytes.len()
                        );
                        let _ = stream.write_all(head.as_bytes()).await;
                        let _ = stream.write_all(&bytes[..64 * 1024]).await;
                        let _ = stream.flush().await;
                        drop(stream);
                    }
                    None => {
                        let _ = stream.write_all(&reply("200 OK", &bytes)).await;
                    }
                }
            });
        }
    });
    format!("http://{addr}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn install_from_a_url_equals_the_source_and_resumes_with_range() {
    let dir = tempdir("url");
    let (source, source_handle, _) = source_shard(&dir, Layout::SingleImage).await;
    let want = signature(&source).await;
    let want_counts = counts(&health(&source).await);
    let repo_dir = dir.join("repo");
    let export = export_snapshot(&source, &repo_dir).await.unwrap();
    let biggest = manifest_of(&export)
        .artifacts
        .iter()
        .map(|a| a.bytes)
        .max()
        .unwrap();
    assert!(
        biggest > 64 * 1024,
        "the fixture must exceed the drop point ({biggest} bytes)"
    );
    let server = Arc::new(FileServer {
        root: repo_dir.clone(),
        bearer: Some("repository-token".into()),
        drop_first: AtomicBool::new(true),
        range_requests: AtomicU64::new(0),
        requests: AtomicU64::new(0),
    });
    let base = serve_files(Arc::clone(&server)).await;
    let (target, target_handle) =
        start_empty_node(config(dir.join("target.tv"), Layout::SingleImage)).await;
    // Without the bearer: refused by name, nothing staged.
    let error = install(&target, Source::Url(base.clone()), "")
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    assert!(error.message().contains("HTTP 401"), "{}", error.message());
    // With it: the first artifact's connection drops after 64 KiB and
    // the client resumes with a Range request; the install equals.
    let installed = install_snapshot_from(
        &target,
        InstallSnapshotFromRequest {
            source: Some(Source::Url(format!("{base}/"))),
            expected_manifest_sha256: export.manifest_sha256.clone(),
            bearer_token: "repository-token".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(installed.num_vectors, DOCS as u64);
    assert!(
        server.range_requests.load(Ordering::Relaxed) >= 1,
        "the interrupted artifact resumed with a Range request"
    );
    assert!(!server.drop_first.load(Ordering::Relaxed));
    assert_eq!(counts(&health(&target).await), want_counts);
    assert_eq!(signature(&target).await, want);
    // A repository that is not there, and a scheme that is not a source.
    let error = install_snapshot_from(
        &target,
        InstallSnapshotFromRequest {
            source: Some(Source::Url(format!("{base}/missing"))),
            expected_manifest_sha256: String::new(),
            bearer_token: "repository-token".into(),
        },
    )
    .await
    .unwrap_err();
    assert!(error.message().contains("HTTP 404"), "{}", error.message());
    let error = install(&target, Source::Url("ftp://nas/repo".into()), "")
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("scheme \"ftp\""),
        "{}",
        error.message()
    );
    target_handle.abort();
    source_handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_layout_exports_installs_reopens_and_never_mixes() {
    let dir = tempdir("segments");
    let (source, source_handle, source_path) = source_shard(&dir, Layout::Segments).await;
    let want = signature(&source).await;
    let want_counts = counts(&health(&source).await);
    let staging = pipestream_search::node::segments_root(&source_path).join("staging/unpublished");
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(
        staging.join("partial-image"),
        b"unpublished compaction output",
    )
    .unwrap();
    let repo_dir = dir.join("repo");
    let export = export_snapshot(&source, &repo_dir).await.unwrap();
    let manifest = manifest_of(&export);
    assert_eq!(manifest.layout, LAYOUT_SEGMENTS);
    assert!(!manifest
        .artifacts
        .iter()
        .any(|artifact| artifact.file.contains("staging")));
    assert!(!repo_dir.join("catalog/staging").exists());
    assert!(manifest.artifact("catalog/segments.json").is_some());
    assert!(
        manifest
            .artifacts
            .iter()
            .filter(|a| a.file.starts_with("catalog/segments/"))
            .count()
            >= 2
    );
    for artifact in &manifest.artifacts {
        let (bytes, sha) = repo::hash_file(&repo_dir.join(&artifact.file)).unwrap();
        assert_eq!((bytes, sha), (artifact.bytes, artifact.sha256.clone()));
    }
    let target_path = dir.join("target.tv");
    let (target, target_handle) =
        start_empty_node(config(target_path.clone(), Layout::Segments)).await;
    let installed = install(
        &target,
        Source::Directory(repo_dir.display().to_string()),
        &export.manifest_sha256,
    )
    .await
    .unwrap();
    assert_eq!(installed.num_vectors, DOCS as u64);
    assert_eq!(installed.num_documents, DOCS as u64);
    assert!(installed.path.ends_with(".segments"), "{}", installed.path);
    assert_eq!(counts(&health(&target).await), want_counts);
    assert_eq!(signature(&target).await, want);
    // Older exports may include a pre-seal standalone image alongside the
    // catalog. Its row count must not replace the catalog's serving rows.
    let legacy = dir.join("legacy-repository");
    let mut legacy_manifest = manifest.clone();
    for artifact in &manifest.artifacts {
        let target = legacy.join(&artifact.file);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::copy(repo_dir.join(&artifact.file), target).unwrap();
    }
    let first_image = manifest
        .artifacts
        .iter()
        .find(|artifact| {
            artifact.file.starts_with("catalog/segments/")
                && artifact.file.ends_with("/vector.index")
        })
        .unwrap();
    legacy_manifest.artifacts.push(
        repo::copy_and_hash(
            &repo_dir.join(&first_image.file),
            &legacy.join("vector.index"),
            "vector.index",
        )
        .unwrap(),
    );
    let (_, legacy_digest) = repo::write_manifest(&legacy, &legacy_manifest).unwrap();
    let installed = install(
        &target,
        Source::Directory(legacy.display().to_string()),
        &legacy_digest,
    )
    .await
    .unwrap();
    assert_eq!(installed.num_vectors, DOCS as u64);
    assert_eq!(signature(&target).await, want);
    // Reopen from disk: the catalog is the layout, nothing converted.
    target_handle.abort();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let reopened =
        NodeServiceImpl::open(config(target_path.clone(), Layout::Segments), None, false).unwrap();
    let (again, again_handle) = serve(reopened).await;
    assert_eq!(counts(&health(&again).await), want_counts);
    assert_eq!(signature(&again).await, want);
    // The peer source carries the catalog too.
    let (peer_target, peer_handle) =
        start_empty_node(config(dir.join("peer.tv"), Layout::Segments)).await;
    install(&peer_target, Source::PeerAddr(again.clone()), "")
        .await
        .unwrap();
    assert_eq!(signature(&peer_target).await, want);
    // Layouts do not mix: a catalog on a single-image shard, and a single
    // image on a populated segment shard, both refuse by name.
    let (single, single_handle) =
        start_empty_node(config(dir.join("single.tv"), Layout::SingleImage)).await;
    let error = install(
        &single,
        Source::Directory(repo_dir.display().to_string()),
        "",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("--layout=single-image"),
        "{}",
        error.message()
    );
    let single_repo = dir.join("single-repo");
    seed(&single).await;
    ingest(&single, 0, 4).await;
    flush(&single).await;
    export_snapshot(&single, &single_repo).await.unwrap();
    let error = install(
        &again,
        Source::Directory(single_repo.display().to_string()),
        "",
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("layouts do not mix"),
        "{}",
        error.message()
    );
    assert_eq!(
        signature(&again).await,
        want,
        "the refused install changed nothing"
    );
    single_handle.abort();
    peer_handle.abort();
    again_handle.abort();
    source_handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_scoring_fingerprint_refuses_by_name() {
    let dir = tempdir("foreign");
    let (source, source_handle, _) = source_shard(&dir, Layout::SingleImage).await;
    let repo_dir = dir.join("repo");
    let export = export_snapshot(&source, &repo_dir).await.unwrap();
    let manifest = manifest_of(&export);
    // A shard seeded with ANOTHER calibration scores in another space.
    let (target, target_handle) =
        start_empty_node(config(dir.join("target.tv"), Layout::SingleImage)).await;
    let other = unit_vectors(2_000, DIM, 0xBAD5_EED0);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &other);
    client(&target)
        .await
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
    let serving = health(&target).await.scoring_fingerprint;
    assert_ne!(serving, manifest.scoring_fingerprint);
    for source in [
        Source::Directory(repo_dir.display().to_string()),
        Source::PeerAddr(source.clone()),
    ] {
        let error = install(&target, source, "").await.unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(
            error.message().contains(&manifest.scoring_fingerprint)
                && error.message().contains(&serving)
                && error.message().contains("same provider state"),
            "{}",
            error.message()
        );
    }
    assert_eq!(health(&target).await.num_vectors, 0);
    target_handle.abort();
    source_handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_manifest_cutoff_is_where_replication_resumes() {
    let dir = tempdir("cutoff");
    let (primary, primary_handle, _) = source_shard(&dir, Layout::SingleImage).await;
    let repo_dir = dir.join("repo");
    let export = export_snapshot(&primary, &repo_dir).await.unwrap();
    let manifest = manifest_of(&export);
    let (replica, replica_handle) =
        start_empty_node(config(dir.join("replica.tv"), Layout::SingleImage)).await;
    install(
        &replica,
        Source::Directory(repo_dir.display().to_string()),
        "",
    )
    .await
    .unwrap();
    // The primary moves on after the export.
    ingest(&primary, DOCS, DOCS + 6).await;
    flush(&primary).await;
    let ahead = health(&primary).await;
    assert_eq!(ahead.num_vectors, (DOCS + 6) as u64);
    assert!(ahead.wal_high_watermark > manifest.wal_high_watermark);
    // Catch-up from exactly the manifest's clock: the tail after the
    // image, nothing before it (a replay of the image's own records
    // would refuse as a vector gap or a partial batch).
    let cursor = ReplicaCursor {
        primary: primary.clone(),
        replica: replica.clone(),
        wal_generation: manifest.wal_generation,
        clock: manifest.wal_high_watermark,
    };
    let advanced = sync_once(&cursor).await.unwrap();
    assert_eq!(advanced.wal_generation, ahead.wal_generation);
    assert_eq!(advanced.clock, ahead.wal_high_watermark);
    assert_eq!(counts(&health(&replica).await), counts(&ahead));
    assert_eq!(signature(&replica).await, signature(&primary).await);
    // Idempotent: the same cursor again changes nothing.
    let again = sync_once(&advanced).await.unwrap();
    assert_eq!(again, advanced);
    assert_eq!(counts(&health(&replica).await), counts(&ahead));
    replica_handle.abort();
    primary_handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_snapshot_refuses_rehashed_misaligned_exact_rows_before_replacing_live_data() {
    let dir = tempdir("exact-shape");
    let (source, source_handle, _) = source_shard(&dir, Layout::Segments).await;
    let repository = dir.join("repository");
    let exported = export_snapshot(&source, &repository).await.unwrap();
    let mut manifest = manifest_of(&exported);
    // The live network source stays the receiver: refused installs must leave
    // its existing query results and row counts intact.
    let before = signature(&source).await;
    for (dim, rows) in [(DIM, DOCS - 1), (DIM + 1, DOCS)] {
        let path = repository.join("vectors.f32");
        pipestream_search::exact_vectors::ExactVectorStore::from_values(dim, vec![0.0; dim * rows])
            .unwrap()
            .write(&path)
            .unwrap();
        let (bytes, sha256) = repo::hash_file(&path).unwrap();
        let artifact = manifest
            .artifacts
            .iter_mut()
            .find(|a| a.file == "vectors.f32")
            .unwrap();
        artifact.bytes = bytes;
        artifact.sha256 = sha256;
        let (_, digest) = repo::write_manifest(&repository, &manifest).unwrap();
        let error = install(
            &source,
            Source::Directory(repository.display().to_string()),
            &digest,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(
            error.message().contains("sidecar shape disagrees"),
            "{error}"
        );
        assert_eq!(signature(&source).await, before);
    }
    source_handle.abort();
    let _ = source_handle.await;
    std::fs::remove_dir_all(dir).unwrap();
}
