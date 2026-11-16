//! `configure-backend`: copy provider construction state between nodes.

use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{ConfigureVectorBackendRequest, GetVectorBackendRequest};

use super::transport::secure_channel;

/// Copy provider-owned construction state from one node to empty peers.
pub(crate) async fn configure_backend(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let get = |key: &str| {
        let prefix = format!("--{key}=");
        args.iter()
            .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
            .ok_or_else(|| format!("configure-backend requires --{key}"))
    };
    let normalize = |s: &str| {
        if s.starts_with("http://") || s.starts_with("https://") {
            s.to_string()
        } else {
            format!("http://{s}")
        }
    };
    let fit_from = normalize(&get("fit-from")?);
    let apply_to: Vec<String> = get("apply-to")?
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(normalize)
        .collect();
    if apply_to.is_empty() {
        return Err("configure-backend requires at least one --apply-to node".into());
    }

    let mut source = NodeServiceClient::new(secure_channel(&fit_from).await?);
    let backend = source
        .get_vector_backend(GetVectorBackendRequest {})
        .await?
        .into_inner();
    let descriptor = backend
        .descriptor
        .ok_or_else(|| format!("{fit_from} reports no vector backend"))?;
    let config = backend
        .config
        .ok_or_else(|| format!("{fit_from} reports no vector backend config"))?;
    println!(
        "fit-from {fit_from}: {} dim {} fingerprint {}, {} vectors",
        descriptor.backend_kind,
        descriptor.dim,
        descriptor.scoring_fingerprint,
        backend.num_vectors
    );

    for addr in &apply_to {
        let mut client = NodeServiceClient::new(secure_channel(addr).await?);
        let resp = client
            .configure_vector_backend(ConfigureVectorBackendRequest {
                dim: descriptor.dim,
                config: Some(config.clone()),
            })
            .await?
            .into_inner();
        println!(
            "apply-to {addr}: {}",
            if resp.already_configured {
                "already configured (idempotent)"
            } else {
                "vector backend configured"
            }
        );
    }
    Ok(())
}
