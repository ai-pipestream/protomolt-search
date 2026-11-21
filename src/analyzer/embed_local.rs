//! Share the configured bundle across embedding requests.
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tonic::Status;

use crate::turbo_bridge::embed::LocalBatchEmbedder;
use crate::turbo_bridge::embed_deadline::EmbedDeadline;
use crate::turbo_bridge::embed_executor::{local_limit, run};

static BUNDLE: Mutex<Option<(PathBuf, LocalBatchEmbedder)>> = Mutex::new(None);

fn cached(path: &Path) -> Option<LocalBatchEmbedder> {
    let handle = BUNDLE.lock().expect("local embedding cache poisoned");
    handle
        .as_ref()
        .filter(|(dir, _)| dir == path)
        .map(|(_, engine)| engine.clone())
}

pub(super) async fn configured(deadline: EmbedDeadline) -> Result<LocalBatchEmbedder, Status> {
    let path = std::env::var_os("TURBO_EMBED_BUNDLE")
        .map(PathBuf::from)
        .ok_or_else(super::missing_bundle)?;
    if let Some(engine) = cached(&path) {
        return Ok(engine);
    }
    run(local_limit(), deadline, || {
        move || {
            if let Some(engine) = cached(&path) {
                return Ok(engine);
            }
            let engine = LocalBatchEmbedder::open(&path, None)?;
            *BUNDLE.lock().expect("local embedding cache poisoned") = Some((path, engine.clone()));
            Ok(engine)
        }
    })
    .await
}
