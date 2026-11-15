//! Push a centrally built generation to a node over one
//! `InstallSnapshot` client stream.

use std::path::{Path, PathBuf};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::{snapshot_chunk, InstallSnapshotResponse, SnapshotChunk, SnapshotManifest};

/// Snapshot payload chunk size. 1 MiB keeps messages far under
/// [`crate::MAX_MESSAGE_BYTES`] while amortizing per-message overhead.
pub const SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;

/// Stream the provider image at `vector_path` (and the `.bm25` sidecar at
/// `bm25_path`, when given) to the node at `addr`, returning the node's
/// install report.
pub async fn install_snapshot(
    addr: &str,
    vector_path: &Path,
    bm25_path: Option<&Path>,
) -> Result<InstallSnapshotResponse, Status> {
    install_snapshot_with_exact(addr, vector_path, None, bm25_path).await
}

/// Stream a complete product generation. The exact-vector sidecar, when
/// present, is sent between the provider image and BM25 bytes.
pub async fn install_snapshot_with_exact(
    addr: &str,
    vector_path: &Path,
    exact_vector_path: Option<&Path>,
    bm25_path: Option<&Path>,
) -> Result<InstallSnapshotResponse, Status> {
    install_snapshot_generation(addr, vector_path, exact_vector_path, bm25_path, None).await
}

/// Stream all files in a product generation, including its optional
/// live-row overlay.
pub async fn install_snapshot_generation(
    addr: &str,
    vector_path: &Path,
    exact_vector_path: Option<&Path>,
    bm25_path: Option<&Path>,
    live_docs_path: Option<&Path>,
) -> Result<InstallSnapshotResponse, Status> {
    let retained = crate::multi_res::sidecar_path(vector_path);
    install_snapshot_generation_with_retained(
        addr,
        vector_path,
        exact_vector_path,
        bm25_path,
        live_docs_path,
        Some(&retained),
    )
    .await
}

/// Stream a generation with an explicitly located retained-vector sidecar.
/// The declaration beside that sidecar is included in the manifest.
pub async fn install_snapshot_generation_with_retained(
    addr: &str,
    vector_path: &Path,
    exact_vector_path: Option<&Path>,
    bm25_path: Option<&Path>,
    live_docs_path: Option<&Path>,
    retained_path: Option<&Path>,
) -> Result<InstallSnapshotResponse, Status> {
    let multi_res_declaration = retained_path
        .map(crate::multi_res_storage::export_declaration)
        .transpose()
        .map_err(|error| {
            Status::failed_precondition(format!("snapshot retained vectors: {error}"))
        })?
        .flatten();
    let retained_path = retained_path.filter(|_| multi_res_declaration.is_some());
    let multi_res_vector_bytes = retained_path
        .map(std::fs::metadata)
        .transpose()
        .map_err(|error| Status::internal(format!("stat retained vectors: {error}")))?
        .map_or(0, |meta| meta.len());
    let manifest = SnapshotManifest {
        multi_res_vector_bytes,
        multi_res_declaration: multi_res_declaration.unwrap_or_default(),
        vector_bytes: optional_len(Some(vector_path))?,
        exact_vector_bytes: optional_len(exact_vector_path)?,
        bm25_bytes: optional_len(bm25_path)?,
        live_docs_bytes: optional_len(live_docs_path)?,
    };
    let paths: Vec<PathBuf> = [
        Some(vector_path),
        exact_vector_path,
        bm25_path,
        live_docs_path,
        retained_path,
    ]
    .into_iter()
    .flatten()
    .map(Path::to_path_buf)
    .collect();
    let chunks = spawn_chunk_sender(manifest, paths);
    let mut client = push_client(addr).await?;
    if multi_res_vector_bytes > 0 {
        let health = client
            .health(crate::pb::HealthRequest {})
            .await?
            .into_inner();
        if health.multi_res_vector_version < 1 {
            return Err(Status::failed_precondition(
                "snapshot target does not support retained vector tiers",
            ));
        }
    }
    Ok(client.install_snapshot(chunks).await?.into_inner())
}

/// The byte length of a generation file, 0 when the generation has none.
fn optional_len(path: Option<&Path>) -> Result<u64, Status> {
    match path {
        Some(p) => Ok(std::fs::metadata(p)
            .map_err(|e| Status::internal(format!("stat {}: {e}", p.display())))?
            .len()),
        None => Ok(0),
    }
}

/// Start the task that sends the manifest and then each file's chunks,
/// returning the stream the install call reads.
fn spawn_chunk_sender(
    manifest: SnapshotManifest,
    paths: Vec<PathBuf>,
) -> ReceiverStream<SnapshotChunk> {
    let (tx, rx) = mpsc::channel::<SnapshotChunk>(2);
    tokio::spawn(async move {
        let manifest = SnapshotChunk {
            payload: Some(snapshot_chunk::Payload::Manifest(manifest)),
        };
        if tx.send(manifest).await.is_err() {
            return;
        }
        for path in paths {
            if !send_file(&tx, &path).await {
                // A local read failure ends the stream early; the node
                // reports it as a truncated snapshot.
                return;
            }
        }
    });
    ReceiverStream::new(rx)
}

/// A channel to the install target under the process-wide client TLS
/// material, with tonic's default windows and message limits.
async fn push_client(addr: &str) -> Result<NodeServiceClient<tonic::transport::Channel>, Status> {
    let endpoint =
        tonic::transport::Endpoint::from_shared(crate::security::process_secure_url(addr))
            .map_err(|e| Status::unavailable(format!("node at {addr}: {e}")))?;
    let endpoint =
        crate::security::secure_endpoint(endpoint).map_err(Status::failed_precondition)?;
    Ok(NodeServiceClient::new(endpoint.connect().await.map_err(
        |e| Status::unavailable(format!("node at {addr}: {e}")),
    )?))
}

/// Read `path` in [`SNAPSHOT_CHUNK_BYTES`] chunks onto the stream.
/// Returns false when the receiver hung up or the file could not be
/// read (the server then sees a truncated snapshot).
async fn send_file(tx: &mpsc::Sender<SnapshotChunk>, path: &Path) -> bool {
    use tokio::io::AsyncReadExt;
    let Ok(mut file) = tokio::fs::File::open(path).await else {
        return false;
    };
    let mut buf = vec![0u8; SNAPSHOT_CHUNK_BYTES];
    loop {
        match file.read(&mut buf).await {
            Ok(0) => return true,
            Ok(n) => {
                let chunk = SnapshotChunk {
                    payload: Some(snapshot_chunk::Payload::Data(buf[..n].to_vec())),
                };
                if tx.send(chunk).await.is_err() {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
}
