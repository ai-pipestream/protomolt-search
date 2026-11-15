//! A snapshot image on its way in: the receive file, its owner, and the
//! completion a replayed final chunk waits on.

use super::*;

impl Drop for OwnedReceive {
    fn drop(&mut self) {
        // Installation renames this unique incoming path into generations.
        // This path never names the published generation or live store.
        let _ = std::fs::remove_dir_all(&self.receive.dir);
        if let Some(completion) = &self.completion {
            completion.finish(Err(Status::unavailable(
                "the snapshot receive was discarded before installation completed",
            )));
        }
    }
}

impl SnapshotCompletion {
    pub(crate) fn new() -> Self {
        Self {
            outcome: tokio::sync::watch::channel(None).0,
        }
    }

    pub(crate) fn finish(&self, result: Result<(), Status>) {
        self.outcome.send_if_modified(|previous| {
            // A durable installation stays successful if the core reply is
            // subsequently lost. A normal core reply may acknowledge a
            // safely declined image after its file was dropped.
            if previous.as_ref().is_some_and(Result::is_ok) {
                return false;
            }
            if previous.is_some() && result.is_err() {
                return false;
            }
            *previous = Some(result);
            true
        });
    }

    pub(crate) fn failed(&self) -> bool {
        self.outcome.borrow().as_ref().is_some_and(Result::is_err)
    }

    pub(crate) async fn wait(&self) -> Result<(), Status> {
        let mut outcome = self.outcome.subscribe();
        loop {
            let ready = outcome.borrow_and_update().clone();
            if let Some(ready) = ready {
                return ready;
            }
            outcome
                .changed()
                .await
                .map_err(|_| Status::unavailable("snapshot finalization closed"))?;
        }
    }
}

impl SnapshotFile {
    pub(super) fn published(file: std::fs::File) -> Self {
        Self {
            file: tokio::fs::File::from_std(file),
            receive: None,
        }
    }

    pub(crate) fn set_completion(&mut self, completion: Arc<SnapshotCompletion>) {
        self.receive
            .as_mut()
            .expect("an incoming snapshot owns its receive")
            .completion = Some(completion);
    }
}

impl tokio::io::AsyncRead for SnapshotFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncRead::poll_read(Pin::new(&mut self.file), cx, buffer)
    }
}

impl tokio::io::AsyncWrite for SnapshotFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.file), cx, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(Pin::new(&mut self.file), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(Pin::new(&mut self.file), cx)
    }
}

impl tokio::io::AsyncSeek for SnapshotFile {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        tokio::io::AsyncSeek::start_seek(Pin::new(&mut self.file), position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        tokio::io::AsyncSeek::poll_complete(Pin::new(&mut self.file), cx)
    }
}

impl ClaimedReceive {
    pub(crate) fn verify_and_into_snapshot(
        mut self,
        staging: &SnapshotStaging,
        meta: &SnapshotMeta<NodeId, BasicNode>,
    ) -> Result<SnapshotFile, Status> {
        self.file
            .sync_all()
            .map_err(|error| Status::internal(format!("snapshot receive: {error}")))?;
        staging.verify(&self.receive.receive.dir, meta)?;
        self.receive.receive.verified = Some(meta.snapshot_id.clone());
        Ok(SnapshotFile {
            file: tokio::fs::File::from_std(self.file),
            receive: Some(self.receive),
        })
    }
}
