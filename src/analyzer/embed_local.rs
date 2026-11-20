//! Share the configured bundle across embedding requests.
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tonic::Status;

use crate::turbo_bridge::embed::{local_permit, EmbedOptions, LocalBatchEmbedder};

static BUNDLE: Mutex<Option<(PathBuf, LocalBatchEmbedder)>> = Mutex::new(None);

fn cached(path: &Path) -> Option<LocalBatchEmbedder> {
    let handle = BUNDLE.lock().expect("local embedding cache poisoned");
    handle
        .as_ref()
        .filter(|(dir, _)| dir == path)
        .map(|(_, engine)| engine.clone())
}

pub(super) async fn configured(options: &EmbedOptions) -> Result<LocalBatchEmbedder, Status> {
    let path = std::env::var_os("TURBO_EMBED_BUNDLE")
        .map(PathBuf::from)
        .ok_or_else(super::missing_bundle)?;
    if let Some(engine) = cached(&path) {
        return Ok(engine);
    }
    let permit = local_permit(options.call.timeout).await?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if let Some(engine) = cached(&path) {
            return Ok(engine);
        }
        let engine = LocalBatchEmbedder::open(&path, None)?;
        *BUNDLE.lock().expect("local embedding cache poisoned") = Some((path, engine.clone()));
        Ok(engine)
    })
    .await
    .map_err(|e| Status::internal(format!("local embedding initialization: {e}")))?
}
