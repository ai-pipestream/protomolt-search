//! Query embedding through a remote `turbo-kserve` endpoint, with the model
//! handshake that pins what the endpoint serves.

#[cfg(feature = "net")]
use std::collections::HashMap;
#[cfg(feature = "net")]
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, OnceLock,
};

use tonic::Status;

#[cfg(feature = "net")]
use super::pool::{remote_client_for, turbo_pool_generation};
#[cfg(not(feature = "net"))]
use super::status::sidecar_needs_net;
#[cfg(feature = "net")]
use super::status::{client_err_to_status, inspect_remote_client_err, inspect_remote_status};
#[cfg(feature = "net")]
use super::DEFAULT_EMBED_MODEL;
use super::{AnalysisBackend, RemoteCallOptions};

/// Embed `text` through `turbo-kserve`'s Open Inference Protocol (`ModelInfer`)
/// into one L2-normalized query vector.
pub async fn embed_text(addr: &str, text: &str) -> Result<Vec<f32>, Status> {
    embed_text_with_options(
        addr,
        text,
        &RemoteCallOptions {
            interactive: true,
            ..Default::default()
        },
    )
    .await
}

/// Embed `text` through `turbo-kserve` with explicit [`RemoteCallOptions`]
/// (mTLS, bearer auth, RPC deadline, and optional [`turbo_client::IndexGenerationHandshake`]
/// model verification via `ModelMetadata`).
pub async fn embed_text_with_options(
    addr: &str,
    text: &str,
    options: &RemoteCallOptions,
) -> Result<Vec<f32>, Status> {
    match AnalysisBackend::parse(addr)? {
        AnalysisBackend::Native | AnalysisBackend::Turbo => {
            return Err(Status::failed_precondition(
                "in-process lexical analysis does not provide neural embeddings without a configured model bundle; configure a turbo-kserve endpoint or local embedding provider",
            ));
        }
        AnalysisBackend::Sidecar(_) => {}
    }
    if text.is_empty() {
        return Err(Status::invalid_argument("empty text"));
    }
    #[cfg(not(feature = "net"))]
    {
        let _ = options;
        Err(sidecar_needs_net(addr))
    }
    #[cfg(feature = "net")]
    {
        embed_remote(addr, text, options).await
    }
}

#[cfg(feature = "net")]
async fn embed_remote(
    addr: &str,
    text: &str,
    options: &RemoteCallOptions,
) -> Result<Vec<f32>, Status> {
    let (clean_addr, model_name, uri_handshake) = parse_embed_endpoint(addr);
    let (remote, generation) = remote_for_generation(&clean_addr, options)?;
    if let Some(hs) = options
        .expected_handshake
        .as_ref()
        .or(uri_handshake.as_ref())
    {
        if hs.bundle.is_some() {
            let result = remote.verify_model_handshake(&model_name, "", hs).await;
            invalidate_cached_model(&clean_addr, &model_name);
            result.map_err(|e| inspect_remote_client_err(&clean_addr, e))?;
        }
    } else {
        enforce_tofu_remote_model_handshake(
            &clean_addr,
            &model_name,
            &remote,
            generation,
            options.timeout,
        )
        .await?;
    }
    let prio = if options.interactive {
        turbo_client::RemotePriority::Interactive
    } else {
        turbo_client::RemotePriority::Bulk
    };
    let (mut vec, dim) = remote
        .embed_texts(
            &model_name,
            &[text],
            Some("query"),
            Some(prio),
            options.timeout,
        )
        .await
        .map_err(|e| inspect_remote_client_err(&clean_addr, e))?;
    if vec.is_empty() || dim == 0 {
        return Err(Status::failed_precondition(
            "turbo-kserve returned no embeddings",
        ));
    }
    vec.truncate(dim);
    let norm = vec
        .iter()
        .map(|&v| f64::from(v) * f64::from(v))
        .sum::<f64>()
        .sqrt();
    if norm == 0.0 || !norm.is_finite() {
        return Err(Status::failed_precondition("embedding pooled to zero"));
    }
    for v in &mut vec {
        *v = (f64::from(*v) / norm) as f32;
    }
    Ok(vec)
}

#[cfg(feature = "net")]
fn remote_for_generation(
    addr: &str,
    options: &RemoteCallOptions,
) -> Result<(turbo_client::RemoteClient, u64), Status> {
    loop {
        let generation = turbo_pool_generation();
        let remote = remote_client_for(addr, options)?;
        if generation == turbo_pool_generation() {
            return Ok((remote, generation));
        }
    }
}

#[cfg(feature = "net")]
#[derive(Default)]
struct VerifiedModel {
    fingerprint: Option<turbo_client::BundleScoringFingerprint>,
    verified: Option<(u64, tokio::runtime::Id, u64)>,
    endpoint: Option<turbo_client::RemoteEndpointConfig>,
}

#[cfg(feature = "net")]
#[derive(Default)]
struct ModelEntry {
    state: tokio::sync::Mutex<VerifiedModel>,
    recheck: AtomicU64,
}

#[cfg(feature = "net")]
type ModelCache = Mutex<HashMap<(String, String), Arc<ModelEntry>>>;

#[cfg(feature = "net")]
static PINNED_REMOTE_MODELS: OnceLock<ModelCache> = OnceLock::new();

#[cfg(feature = "net")]
fn invalidate_cached_model(addr: &str, model: &str) {
    if let Some(cache) = PINNED_REMOTE_MODELS.get() {
        if let Some(entry) = cache
            .lock()
            .expect("pinned remote models poisoned")
            .get(&(addr.to_owned(), model.to_owned()))
        {
            entry.recheck.fetch_add(1, Ordering::AcqRel);
        }
    }
}

#[cfg(feature = "net")]
async fn enforce_tofu_remote_model_handshake(
    clean_addr: &str,
    model_name: &str,
    remote: &turbo_client::RemoteClient,
    generation: u64,
    timeout: Option<std::time::Duration>,
) -> Result<(), Status> {
    let entry = PINNED_REMOTE_MODELS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("pinned remote models poisoned")
        .entry((clean_addr.to_owned(), model_name.to_owned()))
        .or_default()
        .clone();
    let verify = verify_cached_model(&entry, model_name, remote, generation);
    let result = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, verify)
            .await
            .unwrap_or_else(|_| Err(Status::deadline_exceeded("model handshake timed out"))),
        None => verify.await,
    };
    result.map_err(|status| inspect_remote_status(clean_addr, status))
}

#[cfg(feature = "net")]
async fn verify_cached_model(
    entry: &ModelEntry,
    model_name: &str,
    remote: &turbo_client::RemoteClient,
    generation: u64,
) -> Result<(), Status> {
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| Status::failed_precondition("remote embedding requires a Tokio runtime"))?
        .id();
    let mut cached = entry.state.lock().await;
    let recheck = entry.recheck.load(Ordering::Acquire);
    if cached.verified == Some((generation, runtime, recheck))
        && cached
            .endpoint
            .as_ref()
            .is_some_and(|cfg| same_endpoint_identity(cfg, remote.config()))
    {
        return Ok(());
    }
    let expected = turbo_client::IndexGenerationHandshake {
        analysis: None,
        bundle: cached.fingerprint,
    };
    match remote
        .verify_model_handshake(model_name, "", &expected)
        .await
    {
        Ok((_, fingerprint)) => {
            if fingerprint.sha256 != [0; 32] {
                cached.fingerprint = Some(fingerprint);
                cached.verified = Some((generation, runtime, recheck));
                cached.endpoint = Some(remote.config().clone());
            }
            Ok(())
        }
        Err(error) => {
            let status = client_err_to_status(error);
            if status.code() == tonic::Code::Unimplemented && cached.fingerprint.is_none() {
                Ok(())
            } else {
                Err(status)
            }
        }
    }
}

#[cfg(feature = "net")]
fn same_endpoint_identity(
    a: &turbo_client::RemoteEndpointConfig,
    b: &turbo_client::RemoteEndpointConfig,
) -> bool {
    if a.endpoint != b.endpoint || a.bearer_token != b.bearer_token {
        return false;
    }
    #[cfg(feature = "tls")]
    if a.tls != b.tls {
        return false;
    }
    true
}

#[cfg(feature = "net")]
pub(super) fn parse_embed_endpoint(
    addr: &str,
) -> (
    String,
    String,
    Option<turbo_client::IndexGenerationHandshake>,
) {
    let mut model = None;
    let mut sha256 = None;
    let mut dim = None;
    let mut revision = String::new();
    let mut pooling = "mean".to_string();
    let mut normalize = "l2".to_string();

    let base = if let Some((base, query)) = addr.split_once('?') {
        for pair in query.split('&') {
            let Some((k, v)) = pair.split_once('=') else {
                continue;
            };
            let val = v.trim();
            if val.is_empty() {
                continue;
            }
            match k.trim() {
                "model" => model = Some(val.to_string()),
                "sha256" | "manifest_sha256" => sha256 = Some(val.to_string()),
                "dim" => dim = val.parse::<u32>().ok(),
                "revision" => revision = val.to_string(),
                "pooling" => pooling = val.to_string(),
                "normalize" => normalize = val.to_string(),
                _ => {}
            }
        }
        base.to_string()
    } else {
        addr.to_string()
    };

    let resolved_model = model.unwrap_or_else(env_embed_model);
    if sha256.is_none() {
        sha256 = std::env::var("TURBO_MODEL_SHA256")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
    }
    if dim.is_none() {
        dim = std::env::var("TURBO_MODEL_DIM")
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok());
    }

    let hs = sha256.map(|manifest_sha256| {
        let bundle_fp = turbo_client::BundleScoringSpec {
            model_id: resolved_model.clone(),
            revision,
            manifest_sha256,
            task: "embed".to_string(),
            dim: dim.unwrap_or(0),
            pooling,
            normalize,
        }
        .fingerprint();
        turbo_client::IndexGenerationHandshake {
            analysis: None,
            bundle: Some(bundle_fp),
        }
    });

    (base, resolved_model, hs)
}

#[cfg(feature = "net")]
fn env_embed_model() -> String {
    std::env::var("TURBO_EMBED_MODEL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_EMBED_MODEL.to_string())
}
