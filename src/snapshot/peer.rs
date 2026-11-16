//! Node-to-node snapshot calls: the export and install-from clients and
//! the fetcher that stages a peer's `StreamSnapshot`.

use std::path::Path;

use tonic::Status;

use crate::pb::node_service_client::NodeServiceClient;
use crate::pb::{
    snapshot_chunk, ExportSnapshotRequest, ExportSnapshotResponse, InstallSnapshotFromRequest,
    InstallSnapshotResponse, SnapshotChunk, StreamSnapshotRequest,
};
use crate::snapshot_repository::{self as repo, RepositoryManifest};

/// A channel to a node under the process-wide client TLS material.
async fn node_client(addr: &str) -> Result<NodeServiceClient<tonic::transport::Channel>, Status> {
    let endpoint =
        tonic::transport::Endpoint::from_shared(crate::security::process_secure_url(addr))
            .map_err(|e| Status::unavailable(format!("node at {addr}: {e}")))?
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW);
    let endpoint =
        crate::security::secure_endpoint(endpoint).map_err(Status::failed_precondition)?;
    Ok(NodeServiceClient::new(
        endpoint
            .connect()
            .await
            .map_err(|e| Status::unavailable(format!("node at {addr}: {e}")))?,
    )
    .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
    .max_encoding_message_size(crate::MAX_MESSAGE_BYTES))
}

/// Ask the node at `addr` to export its shard into `directory` (a path
/// the NODE can write).
pub async fn export_snapshot(
    addr: &str,
    directory: &Path,
) -> Result<ExportSnapshotResponse, Status> {
    Ok(node_client(addr)
        .await?
        .export_snapshot(ExportSnapshotRequest {
            directory: directory.display().to_string(),
        })
        .await?
        .into_inner())
}

/// Ask the node at `addr` to install a snapshot it fetches itself.
pub async fn install_snapshot_from(
    addr: &str,
    request: InstallSnapshotFromRequest,
) -> Result<InstallSnapshotResponse, Status> {
    Ok(node_client(addr)
        .await?
        .install_snapshot_from(request)
        .await?
        .into_inner())
}

/// Write the manifest file into the staging directory, creating it.
pub(super) async fn stage_manifest(tmp_dir: &Path, bytes: &[u8]) -> Result<(), Status> {
    tokio::fs::create_dir_all(tmp_dir)
        .await
        .map_err(|e| Status::internal(format!("create staging {}: {e}", tmp_dir.display())))?;
    let path = tmp_dir.join(repo::MANIFEST_FILE);
    tokio::fs::write(&path, bytes)
        .await
        .map_err(|e| Status::internal(format!("write {}: {e}", path.display())))
}

/// Pull a peer node's `StreamSnapshot` into the staging directory: the
/// manifest frame first, then every artifact's bytes in manifest order.
/// Returns the manifest and the SHA-256 of its canonical bytes. Byte
/// counts are checked against the manifest as they arrive; the digests
/// are checked by the install.
pub async fn stage_from_peer(
    peer_addr: &str,
    tmp_dir: &Path,
) -> Result<(RepositoryManifest, String), Status> {
    let peer_addr = crate::config::normalize_addr(peer_addr.to_string());
    let mut stream = node_client(&peer_addr)
        .await?
        .stream_snapshot(StreamSnapshotRequest {})
        .await
        .map_err(|e| Status::new(e.code(), format!("peer {peer_addr}: {}", e.message())))?
        .into_inner();
    let manifest = match stream.message().await? {
        Some(SnapshotChunk {
            payload: Some(snapshot_chunk::Payload::Repository(manifest)),
        }) => RepositoryManifest::from_pb(&manifest).map_err(|e| {
            Status::invalid_argument(format!("peer {peer_addr} sent an invalid manifest: {e}"))
        })?,
        _ => {
            return Err(Status::invalid_argument(format!(
                "peer {peer_addr}: the first StreamSnapshot frame must be the repository manifest"
            )))
        }
    };
    let encoded = manifest.encode();
    let sha = turbo_sha256::sha256(&encoded).to_hex();
    stage_manifest(tmp_dir, &encoded).await?;
    let mut frames = PeerFrames {
        stream,
        peer_addr: &peer_addr,
        pending: Vec::new(),
        ended: false,
    };
    for artifact in &manifest.artifacts {
        frames
            .receive(artifact, &tmp_dir.join(&artifact.file))
            .await?;
    }
    frames.finish().await?;
    Ok((manifest, sha))
}

fn receive_error(what: &Path, e: std::io::Error) -> Status {
    Status::internal(format!("snapshot receive {}: {e}", what.display()))
}

/// The data frames that follow a peer's manifest frame, with the bytes
/// of the current frame not yet written to an artifact.
struct PeerFrames<'a> {
    stream: tonic::Streaming<SnapshotChunk>,
    peer_addr: &'a str,
    pending: Vec<u8>,
    ended: bool,
}

impl PeerFrames<'_> {
    /// Write the next `artifact.bytes` of the stream to `path`, refusing a
    /// stream that ends or changes kind before the artifact is whole.
    async fn receive(&mut self, artifact: &repo::Artifact, path: &Path) -> Result<(), Status> {
        use tokio::io::AsyncWriteExt;
        let peer_addr = self.peer_addr;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| receive_error(parent, e))?;
        }
        let mut file = tokio::fs::File::create(path)
            .await
            .map_err(|e| receive_error(path, e))?;
        let mut written = 0u64;
        while written < artifact.bytes {
            if self.pending.is_empty() {
                match self.stream.message().await? {
                    Some(SnapshotChunk {
                        payload: Some(snapshot_chunk::Payload::Data(data)),
                    }) => self.pending = data,
                    Some(_) => {
                        return Err(Status::invalid_argument(format!(
                            "peer {peer_addr}: a frame after the manifest must carry data"
                        )))
                    }
                    None => {
                        self.ended = true;
                        break;
                    }
                }
            }
            let take = (artifact.bytes - written).min(self.pending.len() as u64) as usize;
            file.write_all(&self.pending[..take])
                .await
                .map_err(|e| receive_error(path, e))?;
            self.pending.drain(..take);
            written += take as u64;
        }
        file.sync_all().await.map_err(|e| receive_error(path, e))?;
        if written != artifact.bytes {
            return Err(Status::invalid_argument(format!(
                "truncated snapshot from peer {peer_addr}: artifact {:?} received {written} of \
                 {} bytes",
                artifact.file, artifact.bytes
            )));
        }
        Ok(())
    }

    /// Refuse any data beyond what the manifest declares.
    async fn finish(mut self) -> Result<(), Status> {
        let peer_addr = self.peer_addr;
        if !self.pending.is_empty() {
            return Err(Status::invalid_argument(format!(
                "peer {peer_addr} sent more data than its manifest declares"
            )));
        }
        if !self.ended {
            match self.stream.message().await? {
                None => {}
                Some(SnapshotChunk {
                    payload: Some(snapshot_chunk::Payload::Data(data)),
                }) if data.is_empty() => {}
                Some(_) => {
                    return Err(Status::invalid_argument(format!(
                        "peer {peer_addr} sent more data than its manifest declares"
                    )))
                }
            }
        }
        Ok(())
    }
}
