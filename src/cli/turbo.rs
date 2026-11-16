//! The startup handshake with remote Turbo endpoints.

use pipestream_search::config::Config;

/// The index-generation handshake of the configured model bundle; `None`
/// when no manifest digest pins one.
pub(super) fn startup_handshake(cfg: &Config) -> Option<turbo_client::IndexGenerationHandshake> {
    cfg.turbo_model_sha256.as_ref().map(|manifest_sha256| {
        let model_id = cfg
            .turbo_rerank_model
            .clone()
            .or_else(|| {
                std::env::var("TURBO_EMBED_MODEL")
                    .ok()
                    .filter(|s| !s.trim().is_empty())
            })
            .unwrap_or_else(|| "embed".to_string());
        let dim = cfg
            .turbo_matryoshka_dims
            .last()
            .copied()
            .or(cfg.turbo_matryoshka_active_dim)
            .unwrap_or(0) as u32;
        let bundle_fp = turbo_client::BundleScoringSpec {
            model_id,
            revision: String::new(),
            manifest_sha256: manifest_sha256.clone(),
            task: "embed".to_string(),
            dim,
            pooling: "mean".to_string(),
            normalize: "l2".to_string(),
        }
        .fingerprint();
        turbo_client::IndexGenerationHandshake {
            analysis: None,
            bundle: Some(bundle_fp),
        }
    })
}

/// Verify the handshake against every configured remote endpoint that
/// names an HTTP(S) URL, before anything is served.
pub(super) async fn verify_remote_endpoints(
    cfg: &Config,
    handshake: Option<&turbo_client::IndexGenerationHandshake>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(hs) = handshake else {
        return Ok(());
    };
    let remote_opts = pipestream_search::analyzer::RemoteCallOptions {
        tls: cfg.client_tls.clone(),
        bearer_token: cfg.turbo_bearer_token.clone(),
        interactive: true,
        timeout: None,
        expected_handshake: Some(hs.clone()),
    };
    for remote_addr in cfg
        .analysis_addr
        .iter()
        .chain(cfg.shards.iter().filter_map(|s| s.analysis_addr.as_ref()))
        .chain(cfg.turbo_rerank_endpoint.iter())
        .chain(cfg.turbo_vocab_endpoint.iter())
    {
        if remote_addr.starts_with("http://") || remote_addr.starts_with("https://") {
            pipestream_search::analyzer::connect_verified_turbo_client(
                remote_addr,
                None,
                &remote_opts,
                hs.clone(),
            )
            .await?;
        }
    }
    Ok(())
}
