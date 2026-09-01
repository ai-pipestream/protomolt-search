//! Client side of `NodeService.InstallSnapshot`: push a centrally built
//! shard image (opaque provider bytes plus optional exact-vector and BM25
//! sidecars) to a node
//! over one client stream. This is the bulk-load path for pre-computed corpora:
//! build compatible provider images once, then push each artifact to its shard
//! owner instead of re-embedding per node.
//! See the proto comments on the RPC for the install rules.

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
    let tv_bytes = std::fs::metadata(vector_path)
        .map_err(|e| Status::internal(format!("stat {}: {e}", vector_path.display())))?
        .len();
    let exact_vector_bytes = match exact_vector_path {
        Some(p) => std::fs::metadata(p)
            .map_err(|e| Status::internal(format!("stat {}: {e}", p.display())))?
            .len(),
        None => 0,
    };
    let bm25_bytes = match bm25_path {
        Some(p) => std::fs::metadata(p)
            .map_err(|e| Status::internal(format!("stat {}: {e}", p.display())))?
            .len(),
        None => 0,
    };
    let live_docs_bytes = match live_docs_path {
        Some(p) => std::fs::metadata(p)
            .map_err(|e| Status::internal(format!("stat {}: {e}", p.display())))?
            .len(),
        None => 0,
    };
    let paths: Vec<PathBuf> = [
        Some(vector_path),
        exact_vector_path,
        bm25_path,
        live_docs_path,
    ]
    .into_iter()
    .flatten()
    .map(Path::to_path_buf)
    .collect();

    let (tx, rx) = mpsc::channel::<SnapshotChunk>(2);
    tokio::spawn(async move {
        let manifest = SnapshotChunk {
            payload: Some(snapshot_chunk::Payload::Manifest(SnapshotManifest {
                vector_bytes: tv_bytes,
                bm25_bytes,
                exact_vector_bytes,
                live_docs_bytes,
            })),
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

    let mut client = NodeServiceClient::connect(addr.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("node at {addr}: {e}")))?;
    Ok(client
        .install_snapshot(ReceiverStream::new(rx))
        .await?
        .into_inner())
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
