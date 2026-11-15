//! Query embedding through a remote `turbo-kserve` endpoint, with the model
//! handshake that pins what the endpoint serves.

#[cfg(feature = "net")]
use std::collections::HashMap;
#[cfg(feature = "net")]
use std::sync::{Mutex, OnceLock};

use tonic::Status;

#[cfg(feature = "net")]
use super::pool::remote_client_for;
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
        let (clean_addr, model_name, uri_handshake) = parse_embed_endpoint(addr);
        let remote = remote_client_for(&clean_addr, options)?;
        if let Some(hs) = options
            .expected_handshake
            .as_ref()
            .or(uri_handshake.as_ref())
        {
            if hs.bundle.is_some() {
                remote
                    .verify_model_handshake(&model_name, "", hs)
                    .await
                    .map_err(|e| inspect_remote_client_err(&clean_addr, e))?;
            }
        } else {
            enforce_tofu_remote_model_handshake(&clean_addr, &model_name, &remote).await?;
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
}

#[cfg(feature = "net")]
static PINNED_REMOTE_MODEL_HANDSHAKES: OnceLock<
    Mutex<HashMap<(String, String), turbo_client::IndexGenerationHandshake>>,
> = OnceLock::new();

#[cfg(feature = "net")]
async fn enforce_tofu_remote_model_handshake(
    clean_addr: &str,
    model_name: &str,
    remote: &turbo_client::RemoteClient,
) -> Result<(), Status> {
    let key = (clean_addr.to_string(), model_name.to_string());
    let pinned = PINNED_REMOTE_MODEL_HANDSHAKES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("pinned remote model handshakes poisoned")
        .get(&key)
        .cloned();
    match pinned {
        Some(expected) => {
            remote
                .verify_model_handshake(model_name, "", &expected)
                .await
                .map_err(|e| inspect_remote_client_err(clean_addr, e))?;
            Ok(())
        }
        None => {
            let probe = turbo_client::IndexGenerationHandshake {
                analysis: None,
                bundle: None,
            };
            match remote.verify_model_handshake(model_name, "", &probe).await {
                Ok((_, fp)) => {
                    if fp.sha256 != [0u8; 32] {
                        let mut map = PINNED_REMOTE_MODEL_HANDSHAKES
                            .get_or_init(|| Mutex::new(HashMap::new()))
                            .lock()
                            .expect("pinned remote model handshakes poisoned");
                        match map.get(&key) {
                            Some(existing) => {
                                existing.verify_bundle(&fp).map_err(client_err_to_status)?;
                            }
                            None => {
                                map.insert(
                                    key,
                                    turbo_client::IndexGenerationHandshake {
                                        analysis: None,
                                        bundle: Some(fp),
                                    },
                                );
                            }
                        }
                    }
                    Ok(())
                }
                Err(e) => {
                    let status = client_err_to_status(e);
                    if status.code() == tonic::Code::Unimplemented {
                        Ok(())
                    } else {
                        Err(inspect_remote_status(clean_addr, status))
                    }
                }
            }
        }
    }
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
