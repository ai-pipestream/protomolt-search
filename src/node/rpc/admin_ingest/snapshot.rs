//! Snapshot install, export, and streaming RPCs.

use crate::node::*;

impl NodeServiceImpl {
    /// A streamed provider image has no segment catalog to install; a
    /// shard installs a repository through `InstallSnapshotFrom`.
    pub(in crate::node::rpc) async fn handle_install_snapshot(
        &self,
        _request: Request<Streaming<SnapshotChunk>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        Err(Status::unimplemented(
            "InstallSnapshot streams a bare provider image, which no shard serves; install a \
             segment repository with InstallSnapshotFrom (directory, url or peer_addr)",
        ))
    }

    pub(in crate::node::rpc) async fn handle_export_snapshot(
        &self,
        request: Request<ExportSnapshotRequest>,
    ) -> Result<Response<ExportSnapshotResponse>, Status> {
        crate::metrics::timed(Route::ExportSnapshot, request, |request| async move {
            let directory = PathBuf::from(request.into_inner().directory);
            let service = self.clone();
            tokio::task::spawn_blocking(move || service.export_snapshot_blocking(&directory))
                .await
                .map_err(|e| Status::internal(format!("export task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    pub(in crate::node::rpc) async fn handle_stream_snapshot(
        &self,
        request: Request<StreamSnapshotRequest>,
    ) -> Result<Response<<Self as NodeService>::StreamSnapshotStream>, Status> {
        crate::metrics::timed_stream(Route::StreamSnapshot, request, |_request| async move {
            let path = self.config.index_path.clone().ok_or_else(|| {
                Status::failed_precondition(
                    "shard has no persistence path (index_path); a snapshot export needs one",
                )
            })?;
            let staging = export_staging_dir(&path);
            let service = self.clone();
            let dir = staging.clone();
            let exported =
                tokio::task::spawn_blocking(move || service.export_snapshot_blocking(&dir))
                    .await
                    .map_err(|e| Status::internal(format!("export task failed: {e}")))?;
            let exported = match exported {
                Ok(exported) => exported,
                Err(e) => {
                    let _ = tokio::fs::remove_dir_all(&staging).await;
                    return Err(e);
                }
            };
            let manifest = exported
                .manifest
                .ok_or_else(|| Status::internal("export produced no manifest"))?;
            let artifacts: Vec<PathBuf> = manifest
                .artifacts
                .iter()
                .map(|artifact| staging.join(&artifact.file))
                .collect();
            let (tx, rx) = mpsc::channel::<Result<SnapshotChunk, Status>>(2);
            tokio::spawn(async move {
                let first = SnapshotChunk {
                    payload: Some(snapshot_chunk::Payload::Repository(manifest)),
                };
                if tx.send(Ok(first)).await.is_ok() {
                    for artifact in artifacts {
                        if !stream_file(&tx, &artifact).await {
                            break;
                        }
                    }
                }
                let _ = tokio::fs::remove_dir_all(&staging).await;
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    pub(in crate::node::rpc) async fn handle_install_snapshot_from(
        &self,
        request: Request<InstallSnapshotFromRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        crate::metrics::timed(Route::InstallSnapshotFrom, request, |request| async move {
            use crate::pb::install_snapshot_from_request::Source;
            let path = self.config.index_path.clone().ok_or_else(|| {
                Status::failed_precondition(
                    "shard has no persistence path (index_path); a snapshot install IS persistence",
                )
            })?;
            let req = request.into_inner();
            let source = req.source.ok_or_else(|| {
                Status::invalid_argument(
                    "InstallSnapshotFrom needs a source: directory, url, or peer_addr",
                )
            })?;
            let tmp_dir = generation_tmp_dir(&path);
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            let staged = match source {
            Source::Directory(directory) => {
                let staging = tmp_dir.clone();
                tokio::task::spawn_blocking(move || {
                    Self::stage_from_directory(Path::new(&directory), &staging)
                })
                .await
                .map_err(|e| Status::internal(format!("stage task failed: {e}")))?
            }
            #[cfg(feature = "net")]
            Source::Url(url) => {
                crate::snapshot::stage_from_url(&url, &req.bearer_token, &tmp_dir).await
            }
            #[cfg(feature = "net")]
            Source::PeerAddr(peer) => crate::snapshot::stage_from_peer(&peer, &tmp_dir).await,
            #[cfg(not(feature = "net"))]
            Source::Url(_) | Source::PeerAddr(_) => Err(Status::failed_precondition(
                "this build has no network stack (feature `net` is off); install from a directory",
            )),
        };
            let (manifest, sha) = match staged {
                Ok(staged) => staged,
                Err(e) => {
                    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
                    return Err(e);
                }
            };
            if let Err(e) = repo::check_expected_sha(&req.expected_manifest_sha256, &sha) {
                let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
                return Err(Status::invalid_argument(e));
            }
            let service = self.clone();
            let cleanup = tmp_dir.clone();
            let result = tokio::task::spawn_blocking(move || {
                service.install_staged_repository(&tmp_dir, &manifest)
            })
            .await
            .map_err(|e| Status::internal(format!("install task failed: {e}")))?;
            if result.is_err() {
                let _ = tokio::fs::remove_dir_all(&cleanup).await;
            }
            result.map(Response::new)
        })
        .await
    }
}
